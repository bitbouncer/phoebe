#include <future>
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
#include <boost/make_shared.hpp>
#include <avro/Compiler.hh>
#include <csi_http/encoding/http_rest_avro_json_encoding.h>
#include "csi_avro_utils/utils.h"
#include <phoebe/avro/get_records_request_t.h>
#include "client.h"
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


namespace csi
{
    std::string phoebe_client::status_to_string(int32_t ec)
    {
        switch (ec)
        {
        case SUCCESS: return "SUCCESS";
        case NO_CONNECTION: return "NO_CONNECTION";
        case BAD_RESPONSE: return "BAD_RESPONSE";
        }
        return to_string((csi::http::status_type) ec);
    }

    phoebe_client::phoebe_client(boost::asio::io_service& ios) :
        _ios(ios),
        _http(ios)
    {
    }

    void phoebe_client::async_get(const std::string& address, const std::vector<boost::uuids::uuid>& v, get_value_callback cb)
    {
        std::string uri = "http://" + address + "/test.svante.eu_all_as_avro";
        phoebe::get_records_request_t request;

        for (std::vector<boost::uuids::uuid>::const_iterator i = v.begin(); i != v.end(); ++i)
            request.records.push_back(boost::uuids::to_string(*i));

        _http.perform_async(
            csi::create_avro_json_rest(
            uri,
            request,
            { "Content-Type:avro/json", "Accept:avro/json" },
            std::chrono::milliseconds(100)),
            [this, v, cb](csi::http_client::call_context::handle request)
        {
            if (!request->transport_result())
            {
                cb(get_value_result(NO_CONNECTION, NULL));
                return;
            }

            if (request->http_result() >= 200 && request->http_result() < 300)
            {
                BOOST_LOG_TRIVIAL(info) <<  "phoebe_client::async_get: "        << request->uri() << ":" << request->http_result() << ", duration=" << request->milliseconds() << "(ms)";
                BOOST_LOG_TRIVIAL(trace) << "phoebe_client::async_get: content" << to_string(request->rx_content());
                try
                {
                    boost::shared_ptr<phoebe::get_records_response_t> response(boost::make_shared<phoebe::get_records_response_t>());
                    csi::avro_json_decode(request->rx_content(), *response);
                    cb(get_value_result(SUCCESS, response));
                    return;
                }
                catch (std::exception& e)
                {
                    BOOST_LOG_TRIVIAL(error) << "phoebe_client::async_get: avro_json_decode exception: " << e.what();
                    cb(get_value_result(BAD_RESPONSE, NULL));
                    return;
                }
            }
            BOOST_LOG_TRIVIAL(warning) << "phoebe_client::async_get: " << request->uri() << ":" << request->http_result() << ", duration=" << request->milliseconds() << "(ms)";
            cb(get_value_result(request->http_result(), NULL));
        });
    }

    // csi::phoebe_client::get_value_result phoebe_client::get(const std::vector<boost::uuids::uuid>& v)
    // {
    //     //std::pair<std::shared_ptr<csi::http_client::call_context>, boost::shared_ptr<avro::ValidSchema>> 
    //     std::promise<std::pair<std::shared_ptr<csi::http_client::call_context>, boost::shared_ptr<avro::ValidSchema>>> p;
    //     std::future<std::pair<std::shared_ptr<csi::http_client::call_context>, boost::shared_ptr<avro::ValidSchema>>>  f = p.get_future();
    //     get_schema_by_id(id, [&p](std::shared_ptr<csi::http_client::call_context> call_context, boost::shared_ptr<avro::ValidSchema> schema)
    //     {
    //         std::pair<std::shared_ptr<csi::http_client::call_context>, boost::shared_ptr<avro::ValidSchema>> res(call_context, schema);
    //         p.set_value(res);
    //     });
    //     f.wait();
    //     std::pair<std::shared_ptr<csi::http_client::call_context>, boost::shared_ptr<avro::ValidSchema>> res = f.get();
    //     int32_t http_res = res.first->http_result();
    //     if (http_res >= 200 && http_res < 300)
    //     {
    //         //add to in cache first
    //         return res.second;
    //     }
    //     //exception???
    //     return NULL;
    //}
}; // namespace