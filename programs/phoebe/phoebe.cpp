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
#include <iostream>
#include <iomanip>
#include <string>
#include <assert.h>
#include <thread>
#include <chrono>
#include <sstream>
#include <array>
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
#include <boost/asio.hpp>
#include <boost/filesystem/fstream.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/recursive_mutex.hpp>
#include <boost/bind.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/program_options.hpp>
#include <boost/uuid/string_generator.hpp>
#include <csi_http/server/http_server.h>
#include <csi_http/csi_http.h>

#include <csi_http/encoding/avro_json_encoding.h>
#include <csi_kafka/highlevel_consumer.h>
#include <csi_kafka/internal/utility.h>
#include <phoebe/avro/get_records_request_t.h>
#include <phoebe/avro/get_records_response_t.h>
#include <openssl/md5.h>

#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/uuid_generators.hpp>

size_t _remaining_records = 0;

static boost::uuids::uuid get_md5(const void* data, size_t size)
{
    MD5_CTX ctx;
    MD5_Init(&ctx);
    MD5_Update(&ctx, data, size);
    boost::uuids::uuid uuid;
    MD5_Final(uuid.data, &ctx);
    return uuid;
}

static boost::uuids::string_generator uuid_from_string;

class rest_handler
{
public:
    rest_handler(boost::asio::io_service& ios, const std::string& topic_name) :
        _consumer(ios, topic_name, 500, 1000000),
        _insync(false)
    {
    }

    ~rest_handler() {}

    void connect(const std::vector<csi::kafka::broker_address>& brokers)
    {
        _consumer.connect(brokers);
        _consumer.connect_forever(brokers); // ugly..
        _consumer.set_offset(csi::kafka::earliest_available_offset);
        //_consumer.set_offset(csi::kafka::latest_offsets);

        _consumer.stream_async([this](const boost::system::error_code& ec1, csi::kafka::error_codes ec2, std::shared_ptr<csi::kafka::fetch_response::topic_data::partition_data> response)
        {
            if (ec1 || ec2)
            {
                BOOST_LOG_TRIVIAL(error) << "stream failed ec1::" << ec1 << " ec2" << csi::kafka::to_string(ec2);
                return;
            }

            if (response->error_code)
            {
                BOOST_LOG_TRIVIAL(error) << "stream failed for partition: "  << response->partition_id << " ec:" << csi::kafka::to_string((csi::kafka::error_codes) response->error_code);
                return;
            }

            int partition_id = response->partition_id;
            int last_offset = -1;
            for (std::vector<std::shared_ptr<csi::kafka::basic_message>>::const_iterator i = response->messages.begin(); i != response->messages.end(); ++i)
            {
                //std::map<boost::uuids::uuid, boost::shared_ptr<std::vector<uint8_t>>> data;
                //md5 of key
                if ((*i)->key.is_null())
                {
                    BOOST_LOG_TRIVIAL(warning) << "got key==NULL";
                    continue;
                }

                boost::uuids::uuid key = get_md5((*i)->key.data(), (*i)->key.size());
                //BOOST_LOG_TRIVIAL(info) << "got hash: " << to_string(key);
                
                {
                    // lock access to storage
                    boost::mutex::scoped_lock xxx(_mutex);

                    if ((*i)->value.is_null())
                        _data.erase(key);
                    else
                        _data[key] = boost::make_shared<std::vector<uint8_t>>((*i)->value.value());
                }

                last_offset = (*i)->offset;
            }
            if (last_offset >= 0)
                _last_offset[partition_id] = last_offset;

            _highwater_mark_offset[partition_id] = response->highwater_mark_offset;

            size_t remaining_records=0;
            for (std::map<int, int64_t>::const_iterator i = _highwater_mark_offset.begin(); i != _highwater_mark_offset.end(); ++i)
                remaining_records += (i->second - 1) - _last_offset[i->first];

            if (!_insync && remaining_records==0)
            {
                _insync = true;
                //BOOST_LOG_TRIVIAL(info) << "all partitions in sync";
            }
            else if (_insync && remaining_records>0)
            {
                _insync = false;
            }

            /*
            if (remaining_records > 0)
            {
                BOOST_LOG_TRIVIAL(info) << "not synced, remaining: " << remaining_records;
            }
            */

            _remaining_records = remaining_records;
        });
    }

    inline std::vector<csi::kafka::highlevel_consumer::metrics> get_metrics()
    {
        return _consumer.get_metrics();
    }

    /// Handle a request and produce a reply.
    void handle_request(std::shared_ptr<csi::http::connection> context)
    {
        if (context->request().method() == csi::http::POST)
        {
            BOOST_LOG_TRIVIAL(info) << to_string(context->request().content()) << std::endl;

            phoebe::get_records_request_t request;
            try
            {
                csi::avro_json_decode(context->request().content(), request);
            }
            catch (std::exception& e)
            {
                BOOST_LOG_TRIVIAL(warning) << "avro_json_decode exception: " << e.what();
                context->reply().create(csi::http::bad_request);
                return;
            }

            if (!_insync)
            {
                BOOST_LOG_TRIVIAL(warning) << "rpc when not in sync, service unavailable";
                context->reply().create(csi::http::service_unavailable);
                return;
            }

            try
            {
                phoebe::get_records_response_t response;
                response.records.reserve(request.records.size());
                for (std::vector<std::string>::const_iterator i = request.records.begin(); i != request.records.end(); ++i)
                {
                    boost::uuids::uuid key(uuid_from_string(*i));
                    // lock access to storage
                    boost::mutex::scoped_lock xxx(_mutex);
                    std::map<boost::uuids::uuid, boost::shared_ptr<std::vector<uint8_t>>>::const_iterator item = _data.find(key);
                    if (item != _data.end())
                    {
                        phoebe::response r;
                        r.error_code = 0;
                        r.value = *item->second;
                        response.records.emplace_back(r);
                    }
                    else
                    {
                        phoebe::response r;
                        r.error_code = 0;
                        // no value needed
                        response.records.emplace_back(r);
                    }
                }
                csi::avro_json_encode(response, context->reply().content());
                context->reply().create(csi::http::ok);
                return;
            }
            catch (std::exception& e)
            {
                BOOST_LOG_TRIVIAL(error) << "get values: exception: " << e.what();
                context->reply().create(csi::http::internal_server_error);
                return;
            }
        }
        else
        {
            context->reply().create(csi::http::bad_request);
        }
    }

    boost::mutex                   _mutex;
    csi::kafka::highlevel_consumer _consumer;
    std::map<boost::uuids::uuid, boost::shared_ptr<std::vector<uint8_t>>> _data;
    std::map<int, int64_t>        _highwater_mark_offset;
    std::map<int, int64_t>        _last_offset;
    bool _insync;
};

int main(int argc, char** argv)
{
#ifdef WIN32
    std::string my_address = "127.0.0.1";
#else
    std::string my_address = "0.0.0.0";
#endif
    int32_t http_port = 8082;
    int32_t kafka_port = 9092;
    std::string topic;
    std::vector<csi::kafka::broker_address> brokers;

    boost::program_options::options_description desc("options");
    desc.add_options()
        ("help", "produce help message")
        ("bind_to", boost::program_options::value<std::string>()->default_value(my_address), "bind_to")
        ("topic", boost::program_options::value<std::string>(), "topic")
        ("broker", boost::program_options::value<std::string>(), "broker")
        ;

    boost::program_options::variables_map vm;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);
    boost::program_options::notify(vm);

    boost::log::core::get()->set_filter(boost::log::trivial::severity >= boost::log::trivial::info);

    if (vm.count("help"))
    {
        std::cout << desc << std::endl;
        return 0;
    }

    if (vm.count("bind_to"))
    {
        my_address = vm["bind_to"].as<std::string>();
        size_t last_colon = my_address.find_last_of(':');
        if (last_colon != std::string::npos)
            http_port = atoi(my_address.substr(last_colon + 1).c_str());
        my_address = my_address.substr(0, last_colon);
    }

    if (vm.count("topic"))
    {
        topic = vm["topic"].as<std::string>();
    }
    else
    {
        std::cout << "--topic must be specified" << std::endl;
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

    std::cout << "binding http to: " << my_address << ":" << http_port << std::endl;
    std::cout << "broker(s)      : ";
    for (std::vector<csi::kafka::broker_address>::const_iterator i = brokers.begin(); i != brokers.end(); ++i)
    {
        std::cout << i->host_name << ":" << i->port;
        if (i != brokers.end() - 1)
            std::cout << ", ";
    }
    std::cout << std::endl;
    std::cout << "topic          : " << topic << std::endl;

    boost::asio::io_service ios;
    std::auto_ptr<boost::asio::io_service::work> work(new boost::asio::io_service::work(ios));
    boost::thread bt(boost::bind(&boost::asio::io_service::run, &ios));

    boost::asio::io_service ios_http;
    std::auto_ptr<boost::asio::io_service::work> work_http(new boost::asio::io_service::work(ios_http));
    boost::thread bt_http(boost::bind(&boost::asio::io_service::run, &ios_http));

    try
    {
        rest_handler handler(ios, topic);
        handler.connect(brokers);


        boost::thread do_log([&handler]
        {
            boost::accumulators::accumulator_set<double, boost::accumulators::stats<boost::accumulators::tag::rolling_mean> > acc(boost::accumulators::tag::rolling_window::window_size = 10);
            while (true)
            {
                boost::this_thread::sleep(boost::posix_time::seconds(1));

                std::vector<csi::kafka::highlevel_consumer::metrics>  metrics = handler.get_metrics();
                uint32_t rx_msg_sec_total = 0;
                uint32_t rx_kb_sec_total = 0;
                for (std::vector<csi::kafka::highlevel_consumer::metrics>::const_iterator i = metrics.begin(); i != metrics.end(); ++i)
                {
                    //std::cerr << "\t partiton:" << (*i).partition << "\t" << (*i).rx_msg_sec << " msg/s \t" << ((*i).rx_kb_sec/1024) << "MB/s \troundtrip:" << (*i).rx_roundtrip << " ms" << std::endl;
                    rx_msg_sec_total += (*i).rx_msg_sec;
                    rx_kb_sec_total += (*i).rx_kb_sec;
                }
                BOOST_LOG_TRIVIAL(info) << "RX: " << rx_msg_sec_total << " msg/s \t" << (rx_kb_sec_total / 1024) << "MB/s, consumer lag: " << _remaining_records;
            }
        });

        csi::http::http_server s1(ios_http, my_address, http_port);
        s1.add_handler("/" + topic, [&handler](const std::vector<std::string>&, std::shared_ptr<csi::http::connection> c)
        {
            handler.handle_request(c);
        });

        while (true)
        {
            boost::this_thread::sleep(boost::posix_time::seconds(1000));
        }
    }
    catch (std::exception& e)
    {
        BOOST_LOG_TRIVIAL(error) << "exception: " << e.what() << " : exiting";
    }
    ios.stop();
    ios_http.stop();
    return 0;
}


