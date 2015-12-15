// http://stackoverflow.com/questions/30293400/how-do-i-set-the-boost-logging-severity-level-from-config

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

#include <postgres_asio/postgres_asio.h>
#include <avro/DataFile.hh>
#include <avro/Generic.hh>
#include "avro_postgres.h"

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

enum operation_t { CREATE_TABLE, INSERT_OP, DELETE_OP };

struct sort_functor
{
    bool operator ()(const boost::filesystem::path& a, const boost::filesystem::path & b)
    {
        return a.generic_string() < b.generic_string();
    }
};

int
main(int argc, char** argv)
{
    //std::vector<std::string> keys;
    std::string filename;

    //std::string connect_string = "host=localhost port=5433 user=postgres password=postgres dbname=test";
    std::string connect_string;
    std::string table_name;

    operation_t operation = INSERT_OP;
    bool dry_run = true;
    boost::log::trivial::severity_level log_level;

    boost::program_options::options_description desc("options");
    desc.add_options()
        ("help", "produce help message")
        //("key", boost::program_options::value<std::string>(), "key")
        ("file", boost::program_options::value<std::string>(), "file")
        ("table_name", boost::program_options::value<std::string>(), "table_name")
        ("connect_string", boost::program_options::value<std::string>(), "connect_string")
        ("operation", boost::program_options::value<std::string>(), "[add rm ct] (default - add)")
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

    /*
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
    */

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

                std::cout << itr->path().generic_string() << ' ';
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

    if (vm.count("operation"))
    {
        std::string op = vm["operation"].as<std::string>();

        if (op == "add")
        {
            operation = INSERT_OP;
        }
        else if (op == "rm")
        {
            operation = DELETE_OP;
        }
        else if (op == "ct")
        {
            operation = CREATE_TABLE;
        }
        else
        {
            std::cout << "invalid operation [add, rm]" << std::endl;
            return -1;
        }
    }

    if (vm["write"].as<bool>())
        dry_run = false;

    /*
    std::string key_info = "{ ";
    for (std::vector<std::string>::const_iterator i = keys.begin(); i != keys.end(); ++i)
    {
    key_info += *i;
    if (i != keys.end() - 1)
    key_info += ", ";
    else
    key_info += " }";
    }
    */

    if (vm.count("table_name"))
    {
        table_name = vm["table_name"].as<std::string>();
    }
    else
    {
        std::cout << "--table_name must be specified" << std::endl;
        return -1;
    }

    if (vm.count("connect_string"))
    {
        connect_string = vm["connect_string"].as<std::string>();
    }
    else
    {
        std::cout << "--connect_string must be specified" << std::endl;
        return -1;
    }

    //BOOST_LOG_TRIVIAL(info) << "config, keys                : " << key_info;
    BOOST_LOG_TRIVIAL(info) << "config, table_name          : " << table_name;
    BOOST_LOG_TRIVIAL(info) << "config, connect_string      : " << connect_string;


    //BOOST_LOG_TRIVIAL(info) << "config, key schema name     : " << key_schema_name;
    //BOOST_LOG_TRIVIAL(info) << "config, val schema name     : " << val_schema_name;

    //boost::log::core::get()->set_filter(boost::log::trivial::severity >= boost::log::trivial::info);

    boost::asio::io_service fg_ios;
    boost::asio::io_service bg_ios;
    std::auto_ptr<boost::asio::io_service::work> work2(new boost::asio::io_service::work(fg_ios));
    std::auto_ptr<boost::asio::io_service::work> work1(new boost::asio::io_service::work(bg_ios));
    boost::thread fg(boost::bind(&boost::asio::io_service::run, &fg_ios));
    boost::thread bg(boost::bind(&boost::asio::io_service::run, &bg_ios));

    auto connection = boost::make_shared<postgres_asio::connection>(fg_ios, bg_ios);
    int ec = connection->connect(connect_string);
    if (ec)
    {
        BOOST_LOG_TRIVIAL(error) << connection->trace_id() << " connect failed ec:" << ec << " last_error:" << connection->last_error();
        return -1;
    }

    //std::cerr << std::endl;
    //schema.toJson(std::cerr);
    //std::cerr << std::endl;

    /*
    if (operation == CREATE_TABLE)
    {
    std::string create_statement = avro2sql_create_table_statement(table_name, schema);
    std::cerr << create_statement << std::endl;
    }
    */

    if (operation == INSERT_OP)
    {
        for (std::vector<boost::filesystem::path>::const_iterator i = files.begin(); i != files.end(); ++i)
        {
            BOOST_LOG_TRIVIAL(info) << "adding file: " << i->generic_string().c_str();
            avro::DataFileReader<avro::GenericDatum> dfr(i->generic_string().c_str());
            const avro::ValidSchema& schema = dfr.dataSchema();

            std::cerr << std::endl;
            schema.toJson(std::cerr);
            std::cerr << std::endl;

            avro::GenericDatum datum(schema);
            std::vector<std::string> sql_values;
            while (dfr.read(datum))
            {
                std::string values = avro2sql_values(schema, datum);
                //std::cerr << values << std::endl;

                sql_values.push_back(values);
                if (sql_values.size() > 10000)
                {
                    std::string column_names = avro2sql_column_names(schema, datum);
                    std::string statement = "insert into " + table_name + " " + column_names + " VALUES\n";

                    for (std::vector<std::string>::const_iterator i = sql_values.begin(); i != sql_values.end(); ++i)
                    {
                        const char* ch = i != (sql_values.end() - 1) ? ",\n" : ";\n";
                        statement += *i + ch;
                    }
                    if (!dry_run)
                    {
                        //std::cerr << connection->get_log_id() << " inserting!!" << std::endl;
                        auto res = connection->exec(statement);
                        if (res.first)
                        {
                            BOOST_LOG_TRIVIAL(error) << connection->trace_id() << ", insert failed ec:" << ec << " last_error:" << connection->last_error();
                            return -1;
                        }
                        //std::cerr << connection->get_log_id() << " done!!" << std::endl;
                    }
                    sql_values.clear();
                }
            }

            // write final part
            if (sql_values.size() > 0)
            {
                std::string column_names = avro2sql_column_names(schema, datum);
                std::string statement = "insert into " + table_name + " " + column_names + " VALUES\n";

                for (std::vector<std::string>::const_iterator i = sql_values.begin(); i != sql_values.end(); ++i)
                {
                    const char* ch = i != (sql_values.end() - 1) ? ",\n" : ";\n";
                    statement += *i + ch;
                }

                if (!dry_run)
                {
                    //std::cerr << connection->get_log_id() << " inserting!!" << std::endl;
                    auto res = connection->exec(statement);
                    if (res.first)
                    {
                        BOOST_LOG_TRIVIAL(error) << connection->trace_id() << ", insert failed ec:" << ec << " last_error:" << connection->last_error();
                        return -1;
                    }
                    //std::cerr << connection->get_log_id() << " done!!" << std::endl;
                }
            }
        }
        BOOST_LOG_TRIVIAL(info) << "add files done";
    }

    //start transaction?
    //commit??

    work1.reset();
    work2.reset();
    //bg_ios.stop();
    //fg_ios.stop();
    bg.join();
    fg.join();

}

