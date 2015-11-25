#include <map>
#include <csi_http/client/http_client.h>
#include <avro/Generic.hh>
#include <avro/Schema.hh>

#include <phoebe/avro/get_records_response_t.h>

#pragma once

namespace csi
{
    class phoebe_client
    {
    public:
        enum { SUCCESS = 0, NO_CONNECTION = -1, BAD_RESPONSE = -2 };
        static std::string status_to_string(int32_t);
        typedef std::pair<int32_t, boost::shared_ptr<phoebe::get_records_response_t>>  get_value_result;
        typedef boost::function <void(get_value_result)>                               get_value_callback;

        phoebe_client(boost::asio::io_service& ios, const std::string& address);
        void             async_get(const std::vector<boost::uuids::uuid>& v, get_value_callback);
        get_value_result get(const std::vector<boost::uuids::uuid>& v);
        inline boost::asio::io_service& ios() { return _ios; }
    private:
        typedef boost::function <void(std::shared_ptr<csi::http_client::call_context> call_context, boost::shared_ptr<avro::ValidSchema>)>	get_callback;

        boost::asio::io_service&  _ios;
        csi::http_client          _http;
        std::string               _address;
    };
};

