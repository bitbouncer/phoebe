#include <chrono>
#include <boost/thread.hpp>
#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
#include <boost/filesystem.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <phoebe/client.h>
#include <boost/program_options.hpp>
#include <avro/DataFile.hh>
#include <avro/Generic.hh>
#include <avro/Schema.hh>
#include <avro/ValidSchema.hh>
#include <csi_avro_utils/confluent_codec.h>
#include <csi_avro_utils/utils.h>
#include <csi_avro_utils/hive_schema.h>

#include <openssl/md5.h>
static boost::uuids::uuid get_md5(const void* data, size_t size)
{
    MD5_CTX ctx;
    MD5_Init(&ctx);
    MD5_Update(&ctx, data, size);
    boost::uuids::uuid uuid;
    MD5_Final(uuid.data, &ctx);
    return uuid;
}

struct sort_functor
{
    bool operator ()(const boost::filesystem::path& a, const boost::filesystem::path & b)
    {
        return a.generic_string() < b.generic_string();
    }
};

int main(int argc, char** argv)
{
    boost::log::core::get()->set_filter(boost::log::trivial::severity >= boost::log::trivial::info);

    std::string key_schema_name;
    boost::log::trivial::severity_level log_level;

    boost::program_options::options_description desc("options");
    desc.add_options()
        ("help", "produce help message")
        ("phoebe", boost::program_options::value<std::string>(), "phoebe")
        ("schema_registry", boost::program_options::value<std::string>(), "schema_registry")
        ("key", boost::program_options::value<std::string>(), "key")
        ("key_schema_name", boost::program_options::value<std::string>(), "key_schema_name")
        ("file", boost::program_options::value<std::string>(), "file")
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
    BOOST_LOG_TRIVIAL(info) << "loglevel set to:" << log_level;

    if (vm.count("help"))
    {
        std::cout << desc << std::endl;
        return 0;
    }

    std::string phoebe_uri;
    if (vm.count("phoebe"))
    {
        phoebe_uri = vm["phoebe"].as<std::string>();
    }
    else
    {
        std::cout << "--phoebe must be specified" << std::endl;
        return -1;
    }

    std::string schema_registry_uri;
    if (vm.count("schema_registry"))
    {
        schema_registry_uri = vm["schema_registry"].as<std::string>();
    }
    else
    {
        std::cout << "--phoebe must be specified" << std::endl;
        return -1;
    }
    std::vector<std::string> keys;
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

    if (vm.count("key_schema_name"))
    {
        key_schema_name = vm["key_schema_name"].as<std::string>();
    }
    else
    {
        std::cout << "--key_schema_name must be specified" << std::endl;
        return 0;
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

    std::string filename;
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


    boost::asio::io_service ios;
    std::auto_ptr<boost::asio::io_service::work> work(new boost::asio::io_service::work(ios));
    boost::thread bt(boost::bind(&boost::asio::io_service::run, &ios));

    confluent::registry registry(ios, schema_registry_uri); // should really be a broker list as well
    confluent::codec    avro_codec(registry);

    csi::phoebe_client kv_store(ios, phoebe_uri);

    for (std::vector<boost::filesystem::path>::const_iterator i = files.begin(); i != files.end(); ++i)
    {
        BOOST_LOG_TRIVIAL(info) << "adding file: " << i->generic_string().c_str();
        avro::DataFileReader<avro::GenericDatum> dfr(i->generic_string().c_str());
        const avro::ValidSchema& schema = dfr.dataSchema();

        auto keyschema = csi::avro_hive::get_key_schema(key_schema_name, keys, false, dfr.dataSchema());

        std::cerr << to_string(*keyschema) << std::endl;

        std::string val_schema_name = schema.root()->name().fullname();

        //std::string val_schema_name = schema.root()->name().fullname();
        //std::cout << "key schema name : " << key_schema_name << std::endl;
        //std::cout << "val schema name : " << val_schema_name << std::endl;

        BOOST_LOG_TRIVIAL(trace) << "registering schemas";


        auto key_res = avro_codec.put_schema(key_schema_name, keyschema);
        if (key_res.first != 0)
        {
            BOOST_LOG_TRIVIAL(error) << "registering " << key_schema_name << " at " << schema_registry_uri << " failed, ec:" << confluent::codec::to_string((confluent::codec::error_code_t) key_res.first);
            return -1;
        }

        auto val_res = avro_codec.put_schema(val_schema_name, boost::make_shared<avro::ValidSchema>(dfr.dataSchema()));
        if (val_res.first != 0)
        {
            BOOST_LOG_TRIVIAL(error) << "registering " << val_schema_name << " at " << schema_registry_uri << " failed ec : " << confluent::codec::to_string((confluent::codec::error_code_t) val_res.first);
            return -1;
        }
        int32_t key_id = key_res.second;
        int32_t val_id = val_res.second;
        BOOST_LOG_TRIVIAL(info) << "registering schemas done keyid:" << key_id << " val id: " << val_id;

        try
        {
            avro::GenericDatum datum(schema);
            std::vector<boost::uuids::uuid> messages;
            while (dfr.read(datum))
            {
                auto key = csi::avro_hive::get_key(datum, *keyschema);
                //encode key
                {
                    auto os = avro_codec.encode_nonblock(key_id, key);
                    size_t sz = os->byteCount();
                    auto is = avro::memoryInputStream(*os);
                    avro::StreamReader stream_reader(*is);
                    std::vector<uint8_t> bytes;
                    bytes.resize(sz);
                    stream_reader.readBytes(bytes.data(), sz);
                    messages.push_back(get_md5(bytes.data(), bytes.size()));

                    BOOST_LOG_TRIVIAL(info) << "requesting " << to_string(get_md5(bytes.data(), bytes.size()));
                }

                /*
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
                */

                if (messages.size() > 1)
                {
                    auto result = kv_store.get(messages);
                    if (result.first)
                    {
                        BOOST_LOG_TRIVIAL(error) << csi::phoebe_client::status_to_string(result.first);
                        return -1;
                    }

                    assert(result.second->records.size() == messages.size());
                    //assert(result.second->records[0].error_code == 0);

                    BOOST_LOG_TRIVIAL(info) << "got " << result.second->records[0].value.size() << "bytes ";
                    messages.clear();

                    //for debugging
                    break;
                }


                //std::cerr << to_json(schema, datum) << std::endl;
                //extract key from this
                //avro::GenericDatum key_datum(*keyschema);

                //std::cerr << to_json(*keyschema, *key) << std::endl;
            }
            if (messages.size() > 0)
            {
                auto result = kv_store.get(messages);
                if (result.first)
                {
                    BOOST_LOG_TRIVIAL(error) << csi::phoebe_client::status_to_string(result.first);
                    return -1;
                }

                assert(result.second->records.size() == messages.size());
                //assert(result.second->records[0].error_code == 0);

                BOOST_LOG_TRIVIAL(info) << "got " << result.second->records[0].value.size() << "bytes ";
                messages.clear();
            }
            messages.clear();
        }
        catch (std::exception& e)
        {
            BOOST_LOG_TRIVIAL(error) << "exception " << e.what();
        }
    }

    work.reset();
    ios.stop();

    return EXIT_SUCCESS;
}
