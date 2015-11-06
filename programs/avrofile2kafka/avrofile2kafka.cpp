#include <fstream>
#include <assert.h>
#include <boost/make_shared.hpp>
#include <avro/Encoder.hh>
#include <avro/Decoder.hh>
#include <avro/Generic.hh>
#include <avro/Schema.hh>
#include <avro/ValidSchema.hh>
#include <avro/Compiler.hh>
#include <avro/DataFile.hh>

#include <csi_avro_utils/utils.h>

#include <chrono>
#include <boost/thread.hpp>
#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/rolling_mean.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/program_options.hpp>
#include <boost/filesystem.hpp>
#include <csi_kafka/highlevel_producer.h>
#include <csi_avro_utils/confluent_codec.h>
#include <csi_avro_utils/avro_schema_id_encoding.h>


std::string to_json(const avro::ValidSchema& schema, avro::GenericDatum& datum)
{
    avro::EncoderPtr e = avro::jsonEncoder(schema);
    std::auto_ptr<avro::OutputStream> out = avro::memoryOutputStream();
    e->init(*out);
    avro::encode(*e, datum);
    // push back unused characters to the output stream again... really strange...                         
    // otherwise content_length will be a multiple of 4096
    e->flush();
    return to_string(*out);
}

boost::shared_ptr<avro::Schema> create_hive_column_schema(avro::Type t)
{
    //AVRO_STRING,    /*!< String */
    //    AVRO_BYTES,     /*!< Sequence of variable length bytes data */
    //    AVRO_INT,       /*!< 32-bit integer */
    //    
    //    ,     /*!< Floating point number */
    //    AVRO_DOUBLE,    /*!< Double precision floating point number */
    //    AVRO_BOOL,      /*!< Boolean value */
    //    AVRO_NULL,      /*!< Null */

    //    AVRO_RECORD,    /*!< Record, a sequence of fields */
    //    AVRO_ENUM,      /*!< Enumeration */
    //    AVRO_ARRAY,     /*!< Homogeneous array of some specific type */
    //    AVRO_MAP,       /*!< Homogeneous map from string to some specific type */
    //    AVRO_UNION,     /*!< Union of one or more types */
    //    AVRO_FIXED,     /*!< Fixed number of bytes */

    boost::shared_ptr<avro::Schema>  value_schema;
    switch (t)
    {
    case avro::AVRO_INT:
        value_schema = boost::make_shared<avro::IntSchema>();
        break;
    case avro::AVRO_LONG:
        value_schema = boost::make_shared<avro::LongSchema>();
        break;
    default:
        value_schema = boost::make_shared<avro::StringSchema>();
        break;
    };

    /* Make a union of value_schema with null. Some types are already a union,
    * in which case they must include null as the first branch of the union,
    * and return directly from the function without getting here (otherwise
    * we'd get a union inside a union, which is not valid Avro). */
    boost::shared_ptr<avro::Schema> null_schema = boost::make_shared<avro::NullSchema>();
    boost::shared_ptr<avro::UnionSchema> union_schema = boost::make_shared<avro::UnionSchema>();
    union_schema->addType(*null_schema);
    union_schema->addType(*value_schema);
    return union_schema;
}


boost::shared_ptr<avro::Schema> create_hive_strict_key_schema(avro::Type t)
{
    //AVRO_STRING,    /*!< String */
    //    AVRO_BYTES,     /*!< Sequence of variable length bytes data */
    //    AVRO_INT,       /*!< 32-bit integer */
    //    
    //    ,     /*!< Floating point number */
    //    AVRO_DOUBLE,    /*!< Double precision floating point number */
    //    AVRO_BOOL,      /*!< Boolean value */
    //    AVRO_NULL,      /*!< Null */

    //    AVRO_RECORD,    /*!< Record, a sequence of fields */
    //    AVRO_ENUM,      /*!< Enumeration */
    //    AVRO_ARRAY,     /*!< Homogeneous array of some specific type */
    //    AVRO_MAP,       /*!< Homogeneous map from string to some specific type */
    //    AVRO_UNION,     /*!< Union of one or more types */
    //    AVRO_FIXED,     /*!< Fixed number of bytes */

    boost::shared_ptr<avro::Schema>  value_schema;
    switch (t)
    {
    case avro::AVRO_INT:
        return boost::make_shared<avro::IntSchema>();
        break;
    case avro::AVRO_LONG:
        return boost::make_shared<avro::LongSchema>();
        break;
    default:
        return boost::make_shared<avro::StringSchema>();
        break;
    };
}


boost::shared_ptr<avro::ValidSchema>  get_key_schema(const avro::Name base_name, const std::vector<std::string>& keys, bool allow_null, const avro::ValidSchema& src_schema)
{
    auto r = src_schema.root();
    assert(r->type() == avro::AVRO_RECORD);
    size_t nr_of_leaves = r->leaves();

    boost::shared_ptr<avro::RecordSchema> key_schema = boost::make_shared<avro::RecordSchema>(base_name.fullname() + "_key");
    for (std::vector<std::string>::const_iterator i = keys.begin(); i != keys.end(); ++i)
    {
        for (size_t j = 0; j != nr_of_leaves; ++j)
        {
            auto name = r->nameAt(j);
            if (name == *i)
            {
                auto l = r->leafAt(j);
                if (allow_null)
                    key_schema->addField(*i, *create_hive_column_schema(l->type()));
                else
                    key_schema->addField(*i, *create_hive_strict_key_schema(l->type()));
                break;
            }
        }
    }
    return boost::make_shared<avro::ValidSchema>(*key_schema);
}

boost::shared_ptr<avro::ValidSchema> get_value_schema(const std::string& schema_name, const avro::ValidSchema& src_schema, const avro::ValidSchema& key_schema)
{
    auto r = src_schema.root();
    assert(r->type() == avro::AVRO_RECORD);
    boost::shared_ptr<avro::RecordSchema> value_schema = boost::make_shared<avro::RecordSchema>(schema_name);
    return boost::make_shared<avro::ValidSchema>(*value_schema);
}

boost::shared_ptr<avro::GenericDatum> get_key(avro::GenericDatum& value_datum, const avro::ValidSchema& key_schema)
{
    boost::shared_ptr<avro::GenericDatum> key = boost::make_shared<avro::GenericDatum>(key_schema);
    assert(key_schema.root()->type() == avro::AVRO_RECORD);
    assert(value_datum.type() == avro::AVRO_RECORD);
    size_t nKeyFields = key_schema.root()->leaves();

    avro::GenericRecord& key_record(key->value<avro::GenericRecord>());
    avro::GenericRecord& value_record(value_datum.value<avro::GenericRecord>());

    //size_t nValueFields = value_record.fieldCount();

    for (int i = 0; i < nKeyFields; i++)
    {
        std::string column_name = key_schema.root()->nameAt(i);
        assert(value_record.hasField(column_name));
        assert(key_record.hasField(column_name));
        // we need to handle keys with or without nulls ie union or value
        if (key_record.field(column_name).type() == avro::AVRO_UNION)
        {
            key_record.field(column_name) = value_record.field(column_name);
        }
        else
        {
            avro::GenericUnion& au(value_record.field(column_name).value<avro::GenericUnion>());
            avro::GenericDatum& actual_column_value = au.datum();
            key_record.field(column_name) = actual_column_value;
        }
    }
    return key;
}

enum operation_t { INSERT_OP, DELETE_OP };

int
main(int argc, char** argv)
{
    int32_t kafka_port = 9092;
    std::vector<csi::kafka::broker_address> brokers;
    std::string topic;

    int32_t schema_registry_port = 8081;
    std::vector<csi::kafka::broker_address> schema_registrys;
    std::string used_schema_registry;

    std::vector<std::string> keys;
    std::string filename;
    operation_t operation = INSERT_OP;
    bool dry_run = true;
    std::string key_schema_name;

    boost::log::core::get()->set_filter(boost::log::trivial::severity >= boost::log::trivial::info);
    boost::program_options::options_description desc("options");
    desc.add_options()
        ("help", "produce help message")
        ("topic", boost::program_options::value<std::string>()->default_value(""), "topic")
        ("broker", boost::program_options::value<std::string>(), "broker")
        ("schema_registry", boost::program_options::value<std::string>(), "schema_registry")
        ("schema_registry_port", boost::program_options::value<int>()->default_value(8081), "schema_registry_port")
        ("key", boost::program_options::value<std::string>(), "key")
        ("key_schema_name", boost::program_options::value<std::string>(), "key_schema_name")
        ("file", boost::program_options::value<std::string>(), "file")
        ("operation", boost::program_options::value<std::string>(), "[add rm] (default - insert)")
        ("write,w", boost::program_options::bool_switch()->default_value(false), "write to kafka")
        ;

    boost::program_options::variables_map vm;
    try
    {
        boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);
    }
    catch (std::exception& e)
    {
        std::cout << "bad command line: " << e.what() << std::endl;
        return 0;
    }

    boost::program_options::notify(vm);

    boost::log::core::get()->set_filter(boost::log::trivial::severity >= boost::log::trivial::debug);

    if (vm.count("help"))
    {
        std::cout << desc << std::endl;
        return 0;
    }

    if (vm.count("broker"))
    {
        std::string s = vm["broker"].as<std::string>();
        size_t last_colon = s.find_last_of(':');
        if (last_colon != std::string::npos)
            kafka_port = atoi(s.substr(last_colon + 1).c_str());
        s = s.substr(0, last_colon);

        // now find the brokers...
        size_t last_separator = s.find_last_of(',');
        while (last_separator != std::string::npos)
        {
            std::string host = s.substr(last_separator + 1);
            brokers.push_back(csi::kafka::broker_address(host, kafka_port));
            s = s.substr(0, last_separator);
            last_separator = s.find_last_of(',');
        }
        brokers.push_back(csi::kafka::broker_address(s, kafka_port));
    }
    else
    {
        std::cout << "--broker must be specified" << std::endl;
        return 0;
    }

    if (vm.count("schema_registry_port"))
    {
        schema_registry_port = vm["schema_registry_port"].as<int>();
    }

    if (vm.count("schema_registry"))
    {
        std::string s = vm["schema_registry"].as<std::string>();
        size_t last_colon = s.find_last_of(':');
        if (last_colon != std::string::npos)
            schema_registry_port = atoi(s.substr(last_colon + 1).c_str());
        s = s.substr(0, last_colon);

        // now find the brokers...
        size_t last_separator = s.find_last_of(',');
        while (last_separator != std::string::npos)
        {
            std::string host = s.substr(last_separator + 1);
            schema_registrys.push_back(csi::kafka::broker_address(host, schema_registry_port));
            s = s.substr(0, last_separator);
            last_separator = s.find_last_of(',');
        }
        schema_registrys.push_back(csi::kafka::broker_address(s, schema_registry_port));
    }
    else
    {
        // default - assume registry is running on all kafka brokers
        for (std::vector<csi::kafka::broker_address>::const_iterator i = brokers.begin(); i != brokers.end(); ++i)
        {
            schema_registrys.push_back(csi::kafka::broker_address(i->host_name, schema_registry_port));
        }
    }

    // right now the schema registry class cannot handle severel hosts so just stick to the first one.
    used_schema_registry = schema_registrys[0].host_name + ":" + std::to_string(schema_registrys[0].port);

    if (vm.count("key"))
    {
        std::string s = vm["key"].as<std::string>();
        size_t last_separator = s.find_last_of(',');
        while (last_separator != std::string::npos)
        {
            std::string key = s.substr(last_separator + 1);
            keys.insert(keys.begin(), key);
            s = s.substr(0, last_separator);
            last_separator = s.find_last_of(',');
        }
        keys.insert(keys.begin(), s);
    }
    else
    {
        std::cout << "--key must be specified" << std::endl;
        return 0;
    }

    if (vm.count("file"))
    {
        filename = vm["file"].as<std::string>();

        if (!boost::filesystem::exists(filename))
        {
            std::cout << "file " << filename << " does not exists " << std::endl;
            return -1;
        }
    }
    else
    {
        std::cout << "--file must be specified" << std::endl;
        return -1;
    }
    avro::DataFileReader<avro::GenericDatum> dfr(filename.c_str());

    const avro::ValidSchema& schema = dfr.dataSchema();
    /*
    {
    std::stringstream ss;
    schema.toJson(ss);
    std::cerr << ss.str() << std::endl;
    }
    */

    /*
    {
    std::cerr << "simple keys [key] without nulls" << std::endl;
    std::stringstream ss;
    keyschema->toJson(ss);
    std::cerr << ss.str() << std::endl;
    }
    */

    std::string val_schema_name = schema.root()->name().fullname();

    topic = vm["topic"].as<std::string>();
    if (topic == "")
        topic = val_schema_name;

    if (vm.count("key_schema_name"))
    {
        key_schema_name = vm["key_schema_name"].as<std::string>();
    }
    else
    {
        key_schema_name = topic + "." + "key";
    }

    if (vm.count("operation"))
    {
        std::string op = vm["operation"].as<std::string>();

        if (op == "insert")
        {
            operation = INSERT_OP;
        }
        else if (op == "delete")
        {
            operation = DELETE_OP;
        }
        else
        {
            std::cout << "invalid operation [insert, delete]" << std::endl;
            return -1;
        }
    }

    if (vm["write"].as<bool>())
        dry_run = false;


    // print out our config...
    std::cout << "broker(s)      : ";
    for (std::vector<csi::kafka::broker_address>::const_iterator i = brokers.begin(); i != brokers.end(); ++i)
    {
        std::cout << i->host_name << ":" << i->port;
        if (i != brokers.end() - 1)
            std::cout << ", ";
    }
    std::cout << std::endl;
    std::cout << "topic          : " << topic << std::endl;

    std::cout << "schema registry: ";
    for (std::vector<csi::kafka::broker_address>::const_iterator i = schema_registrys.begin(); i != schema_registrys.end(); ++i)
    {
        std::cout << i->host_name << ":" << i->port;
        if (i != schema_registrys.end() - 1)
            std::cout << ", ";
    }
    std::cout << std::endl;

    std::cout << "used schema registry: " << used_schema_registry << std::endl;

    std::cout << "keys          : { ";
    for (std::vector<std::string>::const_iterator i = keys.begin(); i != keys.end(); ++i)
    {
        std::cout << *i;
        if (i != keys.end() - 1)
            std::cout << ", ";
    }
    std::cout << " }" << std::endl;

    std::cout << "key schema name : " << key_schema_name << std::endl;
    std::cout << "val schema name : " << val_schema_name << std::endl;


    boost::asio::io_service ios;
    std::auto_ptr<boost::asio::io_service::work> work(new boost::asio::io_service::work(ios));
    boost::thread fg(boost::bind(&boost::asio::io_service::run, &ios));

    confluent::registry registry(ios, used_schema_registry); // should really be a broker list as well
    confluent::codec    avro_codec(registry);

    csi::kafka::highlevel_producer producer(ios, topic, -1, 200, 1000000);
    if (!dry_run)
    {
        producer.connect(brokers);
        BOOST_LOG_TRIVIAL(info) << "connected to kafka";
        producer.connect_forever(brokers);
    }

    boost::thread do_log([&producer, dry_run]
    {
        while (true)
        {
            boost::this_thread::sleep(boost::posix_time::seconds(10));

            if (!dry_run)
            {
                std::vector<csi::kafka::highlevel_producer::metrics>  metrics = producer.get_metrics();

                size_t total_queue = 0;
                uint32_t tx_msg_sec_total = 0;
                uint32_t tx_kb_sec_total = 0;
                for (std::vector<csi::kafka::highlevel_producer::metrics>::const_iterator i = metrics.begin(); i != metrics.end(); ++i)
                {
                    total_queue += (*i).msg_in_queue;
                    tx_msg_sec_total += (*i).tx_msg_sec;
                    tx_kb_sec_total += (*i).tx_kb_sec;
                }
                BOOST_LOG_TRIVIAL(info) << "queue:" << total_queue << "\t" << tx_msg_sec_total << " msg/s \t" << (tx_kb_sec_total / 1024) << "MB/s" << std::endl;
            }
        }
    });

    auto keyschema = get_key_schema(key_schema_name, keys, false, dfr.dataSchema());

    if (operation == INSERT_OP)
    {
        //std::string val_schema_name = schema.root()->name().fullname();
        //std::cout << "key schema name : " << key_schema_name << std::endl;
        //std::cout << "val schema name : " << val_schema_name << std::endl;

        BOOST_LOG_TRIVIAL(trace) << "registering schemas";
        auto key_res = avro_codec.put_schema(key_schema_name, keyschema);
        if (key_res.first != 0)
        {
            BOOST_LOG_TRIVIAL(error) << "registering " << key_schema_name << " at " << used_schema_registry << " failed, ec:" << confluent::codec::to_string((confluent::codec::error_code_t) key_res.first);
            return -1;
        }

        auto val_res = avro_codec.put_schema(val_schema_name, boost::make_shared<avro::ValidSchema>(dfr.dataSchema()));
        if (val_res.first != 0)
        {
            BOOST_LOG_TRIVIAL(error) << "registering " << val_schema_name << " at " << used_schema_registry << " failed ec : " << confluent::codec::to_string((confluent::codec::error_code_t) val_res.first);
            return -1;
        }
        int32_t key_id = key_res.second;
        int32_t val_id = val_res.second;
        BOOST_LOG_TRIVIAL(info) << "registering schemas done";

        try
        {
            avro::GenericDatum datum(schema);
            std::vector<std::shared_ptr<csi::kafka::basic_message>> messages;
            while (dfr.read(datum))
            {
                std::shared_ptr<csi::kafka::basic_message> msg(new csi::kafka::basic_message());

                auto key = get_key(datum, *keyschema);

                //encode key
                {
                    auto os = avro_codec.encode_nonblock(key_id, key);
                    size_t sz = os->byteCount();
                    auto is = avro::memoryInputStream(*os);
                    avro::StreamReader stream_reader(*is);
                    msg->key.set_null(false);
                    msg->key.resize(sz);
                    stream_reader.readBytes(msg->key.data(), sz);
                }

                //encode value
                {
                    auto os = avro_codec.encode_nonblock(val_id, datum);
                    size_t sz = os->byteCount();
                    auto is = avro::memoryInputStream(*os);
                    avro::StreamReader stream_reader(*is);
                    msg->value.set_null(false);
                    msg->value.resize(sz);
                    stream_reader.readBytes(msg->value.data(), sz);
                }

                messages.push_back(msg);
                if (messages.size() > 1000)
                {
                    if (!dry_run)
                        producer.send_sync(messages);
                    messages.clear();
                }


                //std::cerr << to_json(schema, datum) << std::endl;
                //extract key from this
                //avro::GenericDatum key_datum(*keyschema);

                //std::cerr << to_json(*keyschema, *key) << std::endl;
            }
            if (messages.size() > 0)
            {
                if (!dry_run)
                    producer.send_sync(messages);
                messages.clear();
            }
        }
        catch (std::exception& e)
        {
            BOOST_LOG_TRIVIAL(error) << "exception " << e.what();
        }
    }

    if (operation == DELETE_OP)
    {

        BOOST_LOG_TRIVIAL(trace) << "registering schema";
        auto key_res = avro_codec.put_schema(key_schema_name, keyschema);
        if (key_res.first != 0)
        {
            BOOST_LOG_TRIVIAL(error) << "registering " << key_schema_name << " at " << used_schema_registry << " failed, ec:" << confluent::codec::to_string((confluent::codec::error_code_t) key_res.first);
            return -1;
        }
        BOOST_LOG_TRIVIAL(info) << "registering schema done";
        int32_t key_id = key_res.second;

        try
        {
            avro::GenericDatum key_datum(*keyschema);
            std::vector<std::shared_ptr<csi::kafka::basic_message>> messages;
            while (dfr.read(key_datum))
            {
                std::shared_ptr<csi::kafka::basic_message> msg(new csi::kafka::basic_message());
                //encode key
                {
                    auto os = avro_codec.encode_nonblock(key_id, key_datum);
                    size_t sz = os->byteCount();
                    auto is = avro::memoryInputStream(*os);
                    avro::StreamReader stream_reader(*is);
                    msg->key.set_null(false);
                    msg->key.resize(sz);
                    stream_reader.readBytes(msg->key.data(), sz);
                }

                //encode value
                {
                    msg->value.set_null(true);
                }

                messages.push_back(msg);
                if (messages.size() > 1000)
                {
                    if (!dry_run)
                        producer.send_sync(messages);
                    messages.clear();
                }
            }
            if (messages.size() > 0)
            {
                if (!dry_run)
                    producer.send_sync(messages);
                messages.clear();
            }
        }
        catch (std::exception& e)
        {
            BOOST_LOG_TRIVIAL(error) << "exception " << e.what();
        }
    }

    BOOST_LOG_TRIVIAL(info) << "stopping threads";
    ios.stop();
    BOOST_LOG_TRIVIAL(info) << "exiting";
    return 0;
}

