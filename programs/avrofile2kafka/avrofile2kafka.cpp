// http://stackoverflow.com/questions/30293400/how-do-i-set-the-boost-logging-severity-level-from-config

#include <fstream>
#include <assert.h>
#include <chrono>
#include <boost/make_shared.hpp>
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
#include <avro/DataFile.hh>
#include <csi_kafka/highlevel_producer.h>
#include <csi_avro_utils/utils.h>
#include <csi_avro_utils/confluent_codec.h>
#include <csi_avro_utils/hive_schema.h>

#include <openssl/md5.h>
/*
static boost::uuids::uuid get_md5(const void* data, size_t size)
{
MD5_CTX ctx;
MD5_Init(&ctx);
MD5_Update(&ctx, data, size);
boost::uuids::uuid uuid;
MD5_Final(uuid.data, &ctx);
return uuid;
}
*/

struct sort_functor
{
    bool operator ()(const boost::filesystem::path& a, const boost::filesystem::path & b)
    {
        return a.generic_string() < b.generic_string();
    }
};

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
    boost::log::trivial::severity_level log_level;

    //boost::log::core::get()->set_filter(boost::log::trivial::severity >= boost::log::trivial::trace);

    boost::program_options::options_description desc("options");
    desc.add_options()
        ("help", "produce help message")
        ("topic", boost::program_options::value<std::string>(), "topic")
        ("broker", boost::program_options::value<std::string>(), "broker")
        ("schema_registry", boost::program_options::value<std::string>(), "schema_registry")
        ("schema_registry_port", boost::program_options::value<int>()->default_value(8081), "schema_registry_port")
        ("key", boost::program_options::value<std::string>(), "key")
        ("key_schema_name", boost::program_options::value<std::string>(), "key_schema_name")
        ("file", boost::program_options::value<std::string>(), "file")
        ("operation", boost::program_options::value<std::string>(), "[insert delete] (default - insert)")
        ("write,w", boost::program_options::bool_switch()->default_value(false), "write to kafka")
        ("log_level", boost::program_options::value<boost::log::trivial::severity_level>(&log_level)->default_value(boost::log::trivial::info), "log level to output");
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

    boost::log::core::get()->set_filter(boost::log::trivial::severity >= log_level);
    BOOST_LOG_TRIVIAL(info) << "loglevel " << log_level;

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

    std::vector<boost::filesystem::path> files;

    if (vm.count("file"))
    {
        filename = vm["file"].as<std::string>();

        if (!boost::filesystem::exists(filename))
        {
            std::cout << "file " << filename << " does not exists " << std::endl;
            return -1;
        }
        if (boost::filesystem::is_directory(filename))
        {
            for (boost::filesystem::directory_iterator itr(filename); itr != boost::filesystem::directory_iterator(); ++itr)
            {

                std::cout << itr->path().filename() << ' '; // display filename only
                if (is_regular_file(itr->status())) std::cout << " [" << file_size(itr->path()) << ']';
                std::cout << '\n';
                files.insert(files.begin(), *itr);
            }
            std::sort(files.begin(), files.end(), sort_functor());
        }
        else
        {
            files.push_back(filename);
        }
    }
    else
    {
        std::cout << "--file must be specified" << std::endl;
        return -1;
    }
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

    //std::string val_schema_name = schema.root()->name().fullname();
    if (vm.count("topic"))
    {
        topic = vm["topic"].as<std::string>();
    }
    else
    {
        std::cout << "--topic must be specified" << std::endl;
        return -1;
    }

    if (vm.count("key_schema_name"))
    {
        key_schema_name = vm["key_schema_name"].as<std::string>();
    }
    else
    {
        key_schema_name = topic + "." + "key";
    }

    //check if key_schema_name is ok.
    try
    {
        avro::Name an(key_schema_name);
    }
    catch (std::exception& e)
    {
        std::cout << "namecheck on key_schema_name: " << key_schema_name << " failed, reason: " << e.what() << std::endl;
        return -1;
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
    std::string broker_info;
    for (std::vector<csi::kafka::broker_address>::const_iterator i = brokers.begin(); i != brokers.end(); ++i)
    {
        broker_info += i->host_name + ":" + std::to_string(i->port);
        if (i != brokers.end() - 1)
            broker_info += ", ";
    }
    BOOST_LOG_TRIVIAL(info) << "config, broker(s)           : " << broker_info;
    BOOST_LOG_TRIVIAL(info) << "config, topic               : " << topic;

    std::string schema_registrys_info;
    for (std::vector<csi::kafka::broker_address>::const_iterator i = schema_registrys.begin(); i != schema_registrys.end(); ++i)
    {
        schema_registrys_info += i->host_name + ":" + std::to_string(i->port);
        if (i != schema_registrys.end() - 1)
            schema_registrys_info += ", ";
    }
    BOOST_LOG_TRIVIAL(info) << "config, schema_registry(s)  : " << schema_registrys_info;
    BOOST_LOG_TRIVIAL(info) << "config, used schema registry: " << used_schema_registry;

    std::string key_info = "{ ";
    for (std::vector<std::string>::const_iterator i = keys.begin(); i != keys.end(); ++i)
    {
        key_info += *i;
        if (i != keys.end() - 1)
            key_info += ", ";
        else
            key_info += " }";
    }

    BOOST_LOG_TRIVIAL(info) << "config, keys                : " << key_info;


    BOOST_LOG_TRIVIAL(info) << "config, key schema name     : " << key_schema_name;


    if (operation == INSERT_OP)
        BOOST_LOG_TRIVIAL(info) << "operation: INSERT";
    else
        BOOST_LOG_TRIVIAL(info) << "operation: DELETE";

    boost::asio::io_service ios;
    std::auto_ptr<boost::asio::io_service::work> work(new boost::asio::io_service::work(ios));
    boost::thread fg(boost::bind(&boost::asio::io_service::run, &ios));

    confluent::registry registry(ios, used_schema_registry); // should really be a broker list as well
    confluent::codec    avro_codec(registry);

    csi::kafka::highlevel_producer producer(ios, topic, -1, 200, 1000000);
    //if (!dry_run)
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



    for (std::vector<boost::filesystem::path>::const_iterator i = files.begin(); i != files.end(); ++i)
    {
        BOOST_LOG_TRIVIAL(info) << "processing file: " << i->generic_string().c_str();
        avro::DataFileReader<avro::GenericDatum> dfr(i->generic_string().c_str());
        const avro::ValidSchema& schema = dfr.dataSchema();

        auto keyschema = csi::avro_hive::get_key_schema(key_schema_name, keys, false, dfr.dataSchema());
        std::string val_schema_name = schema.root()->name().fullname();

        //std::cerr << to_string(*keyschema) << std::endl;

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
        int32_t key_id = key_res.second;
        BOOST_LOG_TRIVIAL(info) << "registering key schema id: " << key_id;

        int32_t val_id = -1;
        // we only need a value schema on insert
        if (operation == INSERT_OP)
        {
            auto val_res = avro_codec.put_schema(val_schema_name, boost::make_shared<avro::ValidSchema>(dfr.dataSchema()));
            if (val_res.first != 0)
            {
                BOOST_LOG_TRIVIAL(error) << "registering " << val_schema_name << " at " << used_schema_registry << " failed ec : " << confluent::codec::to_string((confluent::codec::error_code_t) val_res.first);
                return -1;
            }
            val_id = val_res.second;
            BOOST_LOG_TRIVIAL(info) << "registering val schema id: " << val_id;
        }

        try
        {
            avro::GenericDatum datum(schema);
            std::vector<std::shared_ptr<csi::kafka::basic_message>> messages;
            while (dfr.read(datum))
            {
                std::shared_ptr<csi::kafka::basic_message> msg(new csi::kafka::basic_message());

                auto key = csi::avro_hive::get_key(datum, *keyschema);

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

                if (operation == INSERT_OP)
                {
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
                }
                else // DELETE
                {
                    msg->value.set_null(true);
                }

                messages.push_back(msg);
                if (messages.size() > 100)
                {
                    if (!dry_run)
                    {
                        producer.send_sync(messages);
                    }
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
    work.reset();
    ios.stop();
    BOOST_LOG_TRIVIAL(info) << "exiting";
    return 0;
}

