# Java 实现的简易 Redis 数据库

## 已实现功能

### 基础功能

- [x] 1. Resp 协议

- [x] 2. Netty 建立连接

- [x] 3. 数据结构实现

- [x] 4. RedisString 命令实现
    - [x] SET key value [EX seconds] [PX milliseconds] [NX|XX]
    - [x] GET key
    - [x] DEL key
    - [x] EXISTS key
    - [x] EXPIRE key seconds
    - [x] TTL key

- [x] 5. RedisSet 命令实现
    - [x] SADD key member [member ...]
    - [x] SMEMBERS key
    - [x] SISMEMBER key member
    - [x] SCARD key
    - [x] SREM key member [member ...]
    - [x] SPOP key [count]

- [x] 6. RedisHash 命令实现
    - [x] HSET key field value [field value ...]
    - [x] HGET key field
    - [x] HMSET key field value [field value ...]
    - [x] HMGET key field [field ...]
    - [x] HGETALL key
    - [x] HDEL key field [field ...]
    - [x] HEXISTS key field
    - [x] HLEN key

- [x] 7. RedisList 命令实现
    - [x] LPUSH key element [element ...]
    - [x] RPUSH key element [element ...]
    - [x] LPOP key
    - [x] RPOP key
    - [x] LLEN key
    - [x] LRANGE key start stop
    - [x] LREM key value

- [x] 8. RedisZset 命令实现
    - [x] ZADD key score member [score member ...]
    - [x] ZRANGE key start stop [WITHSCORES]
    - [x] ZREVRANGE key start stop [WITHSCORES]
    - [x] ZRANGEBYSCORE key min max [WITHSCORES]
    - [x] ZREM key member [member ...]
    - [x] ZCARD key
    - [x] ZSCORE key member
