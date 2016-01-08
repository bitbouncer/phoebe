#include <boost/thread.hpp>
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
#include <boost/program_options.hpp>
#include <boost/endian/arithmetic.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <csi_kafka/kafka.h>
#include <csi_kafka/highlevel_consumer.h>
#include <csi_kafka/highlevel_producer.h>

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



int main(int argc, char** argv)
{
    boost::log::trivial::severity_level log_level;
    boost::program_options::options_description desc("options");
    desc.add_options()
        ("help", "produce help message")
        ("topic", boost::program_options::value<std::string>(), "topic")
        ("broker", boost::program_options::value<std::string>(), "broker")
        ("partition", boost::program_options::value<std::string>(), "partition")
        ("key_schema_id", boost::program_options::value<std::string>(), "key_schema_id")
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

    int32_t kafka_port = 9092;
    std::vector<csi::kafka::broker_address> brokers;
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

    int32_t schema_registry_port = 8081;
    std::vector<csi::kafka::broker_address> schema_registrys;
    std::string used_schema_registry;

    std::string topic;
    if (vm.count("topic"))
    {
        topic = vm["topic"].as<std::string>();
    }
    else
    {
        std::cout << "--topic must be specified" << std::endl;
        return -1;
    }


    std::vector<int> key_schemas;
    bool delete_all = false;
    if (vm.count("key_schema_id"))
    {
        std::string s = vm["key_schema_id"].as<std::string>();

        //special case *
        if (s == "*")
        {
            delete_all = true;
        }
        else
        {
            // now find the brokers...
            size_t last_separator = s.find_last_of(',');
            while (last_separator != std::string::npos)
            {
                std::string token = s.substr(last_separator + 1);
                key_schemas.push_back(atoi(token.c_str()));
                s = s.substr(0, last_separator);
                last_separator = s.find_last_of(',');
            }
            key_schemas.push_back(atoi(s.c_str()));
        }
    }
    else
    {
        std::cout << "--key_schema_id must be specified" << std::endl;
        return 0;
    }

    std::vector<int> partition_mask;
    if (vm.count("partition"))
    {
        std::string s = vm["partition"].as<std::string>();

        //special case *
        if (s == "*")
        {
        }
        else
        {
            // now find the brokers...
            size_t last_separator = s.find_last_of(',');
            while (last_separator != std::string::npos)
            {
                std::string token = s.substr(last_separator + 1);
                partition_mask.push_back(atoi(token.c_str()));
                s = s.substr(0, last_separator);
                last_separator = s.find_last_of(',');
            }
            partition_mask.push_back(atoi(s.c_str()));
        }
    }

    bool dry_run = true;
    if (vm["write"].as<bool>())
        dry_run = false;

    boost::asio::io_service io_service;
    std::auto_ptr<boost::asio::io_service::work> work(new boost::asio::io_service::work(io_service));
    boost::thread bt(boost::bind(&boost::asio::io_service::run, &io_service));

    csi::kafka::highlevel_consumer consumer(io_service, topic, partition_mask, 500, 1000000);
    csi::kafka::highlevel_producer producer(io_service, topic, -1, 500, 1000000);

    consumer.connect(brokers);
    //std::vector<int64_t> result = consumer.get_offsets();

    consumer.connect_forever(brokers);

    {
        producer.connect(brokers);
        BOOST_LOG_TRIVIAL(info) << "connected to kafka";
        producer.connect_forever(brokers);
    }


    std::map<int, int64_t> highwater_mark_offset;
    consumer.set_offset(csi::kafka::latest_offsets);
    // this is assuming to much - what if anything goes wrong...
    auto r = consumer.fetch();
    for (std::vector<csi::kafka::rpc_result<csi::kafka::fetch_response>>::const_iterator i = r.begin(); i != r.end(); ++i)
    {
        if (i->ec)
            continue; // or die??

        for (std::vector<csi::kafka::fetch_response::topic_data>::const_iterator j = (*i)->topics.begin(); j != (*i)->topics.end(); ++j)
        {
            for (std::vector<std::shared_ptr<csi::kafka::fetch_response::topic_data::partition_data>>::const_iterator k = j->partitions.begin(); k != j->partitions.end(); ++k)
            {
                if ((*k)->error_code)
                    continue; // or die??

                highwater_mark_offset[(*k)->partition_id] = (*k)->highwater_mark_offset;
            }
        }
    }

    consumer.set_offset(csi::kafka::earliest_available_offset);

    std::map<int, int64_t> last_offset;
    int64_t _remaining_records = 1;

    std::map<boost::uuids::uuid, std::shared_ptr<csi::kafka::basic_message>> _to_delete;

    consumer.stream_async([delete_all, key_schemas, &last_offset, &highwater_mark_offset, &_remaining_records, &_to_delete](const boost::system::error_code& ec1, csi::kafka::error_codes ec2, std::shared_ptr<csi::kafka::fetch_response::topic_data::partition_data> response)
    {
        if (ec1 || ec2)
        {
            BOOST_LOG_TRIVIAL(error) << "stream failed ec1::" << ec1 << " ec2" << csi::kafka::to_string(ec2);
            return;
        }

        if (response->error_code)
        {
            BOOST_LOG_TRIVIAL(error) << "stream failed for partition: " << response->partition_id << " ec:" << csi::kafka::to_string((csi::kafka::error_codes) response->error_code);
            return;
        }
        int partition_id = response->partition_id;
        int64_t lo = -1;
        for (std::vector<std::shared_ptr<csi::kafka::basic_message>>::const_iterator i = response->messages.begin(); i != response->messages.end(); ++i)
        {
            if ((*i)->key.is_null())
            {
                //BOOST_LOG_TRIVIAL(warning) << "got key==NULL";
                continue;
            }

            if ((*i)->key.size() < 4)
            {
                BOOST_LOG_TRIVIAL(warning) << "got keysize==" << (*i)->key.size();
                continue;
            }

            int32_t be;
            memcpy(&be, (*i)->key.data(), 4);
            int32_t key_schema_id = boost::endian::big_to_native<int32_t>(be);

            // should we kill this schema?
            if (delete_all || std::find(std::begin(key_schemas), std::end(key_schemas), key_schema_id) != std::end(key_schemas))
            {
                boost::uuids::uuid key = get_md5((*i)->key.data(), (*i)->key.size());

                //not already dead
                if (!(*i)->value.is_null())
                {
                    std::map<boost::uuids::uuid, std::shared_ptr<csi::kafka::basic_message>>::iterator item = _to_delete.find(key);
                    if (item == _to_delete.end())
                    {
                        std::shared_ptr<csi::kafka::basic_message> msg(new csi::kafka::basic_message());
                        msg->key = (*i)->key;
                        msg->value.set_null(true);
                        msg->partition = partition_id; // make sure we write the same partion that we got the message from...
                        _to_delete[key] = msg;
                    }
                }
                else
                {
                    //we must search map to se if we should refrain from deleting the item since a deleete marker is already on kafka. (the previous messages has not yet been removed by compaction)
                    //std::map<boost::uuids::uuid, std::shared_ptr<csi::kafka::basic_message>>
                    std::map<boost::uuids::uuid, std::shared_ptr<csi::kafka::basic_message>>::iterator item = _to_delete.find(key);
                    if (item != _to_delete.end())
                    {
                        _to_delete.erase(item);
                    }
                }
            }
            lo = (*i)->offset;
        }
        if (lo >= 0)
            last_offset[partition_id] = lo;

        int64_t remaining_records = 0;
        for (std::map<int, int64_t>::const_iterator i = highwater_mark_offset.begin(); i != highwater_mark_offset.end(); ++i)
            remaining_records += ((int64_t)(i->second - 1)) - (int64_t)last_offset[i->first];
        _remaining_records = remaining_records;
    });

    while (true)
    {
        boost::this_thread::sleep(boost::posix_time::seconds(1));
        BOOST_LOG_TRIVIAL(info) << " to be deleted: " << _to_delete.size() << ", remaining: " << _remaining_records;
        if (_remaining_records <= 0)
            break;
    }
    BOOST_LOG_TRIVIAL(info) << "consumer finished";
    consumer.close();
    BOOST_LOG_TRIVIAL(info) << "consumer closed";

    BOOST_LOG_TRIVIAL(info) << "sending delete messages";

    std::vector<std::shared_ptr<csi::kafka::basic_message>> messages;
    for (std::map<boost::uuids::uuid, std::shared_ptr<csi::kafka::basic_message>>::iterator i = _to_delete.begin(); i != _to_delete.end(); ++i)
    {
        messages.push_back(i->second);
    }
    if (messages.size())
    {
        if (!dry_run)
        {
            producer.send_sync(messages);
        }
        else
        {
            std::cout << "dry run - should write " << messages.size() << " add -w to delete from kafka" << std::endl;
        }
    }
    else
    {
        std::cout << "uptodate - nothing to do" << std::endl;
    }
    BOOST_LOG_TRIVIAL(info) << "producer finished";
    producer.close();
    BOOST_LOG_TRIVIAL(info) << "producer closed";
    boost::this_thread::sleep(boost::posix_time::seconds(5));
    BOOST_LOG_TRIVIAL(info) << "done";
    work.reset();
    io_service.stop();
    return EXIT_SUCCESS;
}
