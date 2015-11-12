#include <boost/thread.hpp>
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
#include <boost/program_options.hpp>
#include <boost/endian/arithmetic.hpp>

#include <csi_kafka/kafka.h>
#include <csi_kafka/highlevel_consumer.h>

int main(int argc, char** argv)
{
    boost::log::trivial::severity_level log_level;
    boost::program_options::options_description desc("options");
    desc.add_options()
        ("help", "produce help message")
        ("topic", boost::program_options::value<std::string>(), "topic")
        ("broker", boost::program_options::value<std::string>(), "broker")
        ("schema_registry", boost::program_options::value<std::string>(), "schema_registry")
        ("schema_registry_port", boost::program_options::value<int>()->default_value(8081), "schema_registry_port")
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

    boost::asio::io_service io_service;
    std::auto_ptr<boost::asio::io_service::work> work(new boost::asio::io_service::work(io_service));
    boost::thread bt(boost::bind(&boost::asio::io_service::run, &io_service));
    
    csi::kafka::highlevel_consumer consumer(io_service, topic, 500, 1000000);

    consumer.connect(brokers);
    //std::vector<int64_t> result = consumer.get_offsets();

    consumer.connect_forever(brokers);

    std::map<int, int64_t> highwater_mark_offset;
    consumer.set_offset(csi::kafka::latest_offsets);
    std::vector<csi::kafka::highlevel_consumer::fetch_response> response = consumer.fetch();
    
    for (std::vector<csi::kafka::highlevel_consumer::fetch_response>::const_iterator i = response.begin(); i != response.end(); ++i)
        highwater_mark_offset[i->data->partition_id] = i->data->highwater_mark_offset;

    //test back all 2 
    std::map<int, int64_t> start_offset = highwater_mark_offset;
    for (std::map<int, int64_t>::iterator i = start_offset.begin(); i != start_offset.end(); ++i)
        i->second -= 2;

    consumer.set_offset(start_offset);

    //boost::thread do_log([&consumer]
    //{
    //    boost::accumulators::accumulator_set<double, boost::accumulators::stats<boost::accumulators::tag::rolling_mean> > acc(boost::accumulators::tag::rolling_window::window_size = 10);
    //    while (true)
    //    {
    //        boost::this_thread::sleep(boost::posix_time::seconds(1));

    //        std::vector<csi::kafka::highlevel_consumer::metrics>  metrics = consumer.get_metrics();
    //        uint32_t rx_msg_sec_total = 0;
    //        uint32_t rx_kb_sec_total = 0;
    //        for (std::vector<csi::kafka::highlevel_consumer::metrics>::const_iterator i = metrics.begin(); i != metrics.end(); ++i)
    //        {
    //            //std::cerr << "\t partiton:" << (*i).partition << "\t" << (*i).rx_msg_sec << " msg/s \t" << ((*i).rx_kb_sec/1024) << "MB/s \troundtrip:" << (*i).rx_roundtrip << " ms" << std::endl;
    //            rx_msg_sec_total += (*i).rx_msg_sec;
    //            rx_kb_sec_total += (*i).rx_kb_sec;
    //        }
    //        BOOST_LOG_TRIVIAL(info) << "RX: " << rx_msg_sec_total << " msg/s \t" << (rx_kb_sec_total / 1024) << "MB/s";
    //    }
    //});

    std::map<int, int64_t> last_offset;
    std::map<int, int64_t> key_schema_count;
    std::map<int, int64_t> value_schema_count;

    consumer.stream_async([&key_schema_count, &value_schema_count, &last_offset, &highwater_mark_offset](const boost::system::error_code& ec1, csi::kafka::error_codes ec2, std::shared_ptr<csi::kafka::fetch_response::topic_data::partition_data> response)
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
        int lo = -1;
        for (std::vector<std::shared_ptr<csi::kafka::basic_message>>::const_iterator i = response->messages.begin(); i != response->messages.end(); ++i)
        {
            //std::map<boost::uuids::uuid, boost::shared_ptr<std::vector<uint8_t>>> data;
            //md5 of key
            if ((*i)->key.is_null())
            {
                BOOST_LOG_TRIVIAL(warning) << "got key==NULL";
                continue;
            }

            if ((*i)->key.size() < 4)
            {
                BOOST_LOG_TRIVIAL(warning) << "got keysize==" << (*i)->key.size();
                continue;
            }

            const uint8_t* p = (*i)->key.data();

            const uint8_t b0 = p[0];
            const uint8_t b1 = p[1];
            const uint8_t b2 = p[2];
            const uint8_t b3 = p[3];

            int32_t be;
            memcpy(&be, (*i)->key.data(), 4);
            int32_t key_schema_id = boost::endian::big_to_native<int32_t>(be);

            if (!(*i)->value.is_null())
            {
                if ((*i)->value.size() < 4)
                {
                    BOOST_LOG_TRIVIAL(warning) << "got valuesize==" << (*i)->value.size();
                    continue;
                }

                int32_t be;
                memcpy(&be, (*i)->value.data(), 4);
                int32_t value_schema_id = boost::endian::big_to_native<int32_t>(be);

                if (key_schema_count.find(key_schema_id) == key_schema_count.end())
                    key_schema_count[key_schema_id] = 1;
                else
                    key_schema_count[key_schema_id]++;

                if (value_schema_count.find(value_schema_id) == value_schema_count.end())
                    value_schema_count[value_schema_id] = 1;
                else
                    value_schema_count[value_schema_id]++;
            }
            lo = (*i)->offset;
        }
        if (lo >= 0)
            last_offset[partition_id] = lo;

        highwater_mark_offset[partition_id] = response->highwater_mark_offset;

        size_t remaining_records = 0;
        for (std::map<int, int64_t>::const_iterator i = highwater_mark_offset.begin(); i != highwater_mark_offset.end(); ++i)
            remaining_records += (i->second - 1) - last_offset[i->first];



        if (remaining_records == 0)
        {
            std::cout << "key schemas" << std::endl;
            for (std::map<int, int64_t>::const_iterator i = key_schema_count.begin(); i != key_schema_count.end(); ++i)
            {
                std::cout << i->first << ", " << i->second << std::endl;
            }

            std::cout << std::endl;
            std::cout << "value schemas" << std::endl;
            for (std::map<int, int64_t>::const_iterator i = value_schema_count.begin(); i != value_schema_count.end(); ++i)
            {
                std::cout << i->first << ", " << i->second << std::endl;
            }

        }
    });

    while (true)
        boost::this_thread::sleep(boost::posix_time::seconds(30));

    consumer.close();

    work.reset();
    io_service.stop();

    return EXIT_SUCCESS;
}