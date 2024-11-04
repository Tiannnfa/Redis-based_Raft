#pragma once
#include <memory>
#include <boost/asio.hpp>
#include <hiredis/hiredis.h>
#include <raft-kv/common/bytebuffer.h>

namespace kv
{

    class RedisStore;

    // If the original `shared_from_this()`-enabled object was created with a `std::shared_ptr`,
    // this ensures the object remains alive for the duration of the asynchronous operation,
    // even if all other `std::shared_ptr` references to the object go out of scope.  
    // The asynchronous handler captures a `shared_ptr` to `self`, 
    // thus extending the object’s lifespan until the handler itself is done executing.
    class RedisSession : public std::enable_shared_from_this<RedisSession>
    {
    public:
        explicit RedisSession(RedisStore *server, boost::asio::io_service &io_service);

        ~RedisSession()
        {
            redisReaderFree(reader_);
        }

        void start();

        void handle_read(size_t bytes);

        void on_redis_reply(struct redisReply *reply);

        void send_reply(const char *data, uint32_t len);

        void start_send();

        static void ping_command(std::shared_ptr<RedisSession> self, struct redisReply *reply);

        static void get_command(std::shared_ptr<RedisSession> self, struct redisReply *reply);

        static void set_command(std::shared_ptr<RedisSession> self, struct redisReply *reply);

        static void del_command(std::shared_ptr<RedisSession> self, struct redisReply *reply);

        static void keys_command(std::shared_ptr<RedisSession> self, struct redisReply *reply);

    public:
        bool quit_;
        RedisStore *server_;
        boost::asio::ip::tcp::socket socket_;
        std::vector<uint8_t> read_buffer_;
        redisReader *reader_;
        ByteBuffer send_buffer_;
    };
    typedef std::shared_ptr<RedisSession> RedisSessionPtr;

}