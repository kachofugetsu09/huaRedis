# Java 实现的简易 Redis 数据库

## 已实现功能

### 基础功能

#### 1. Resp 协议

#### 2. Netty 建立连接

#### 3. 数据结构实现

#### [x] 4. RedisString 命令实现
- [x] SET key value [EX seconds] [PX milliseconds] [NX|XX]
- [x] GET key
- [x] DEL key
- [x] EXISTS key
- [x] EXPIRE key seconds
- [x] TTL key

#### [x] 5. RedisSet 命令实现
- [x] SADD key member [member ...]
- [x] SMEMBERS key
- [x] SISMEMBER key member
- [x] SCARD key
- [x] SREM key member [member ...]
- [x] SPOP key [count]

#### [x] 6. RedisHash 命令实现
- [x] HSET key field value [field value ...]
- [x] HGET key field
- [x] HMSET key field value [field value ...]
- [x] HMGET key field [field ...]
- [x] HGETALL key
- [x] HDEL key field [field ...]
- [x] HEXISTS key field
- [x] HLEN key

#### [ ] 7. RedisList 命令实现
- [ ] LPUSH key element [element ...]
- [ ] RPUSH key element [element ...]
- [ ] LPOP key
- [ ] RPOP key
- [ ] LLEN key
- [ ] LRANGE key start stop

#### [ ] 8. RedisZset 命令实现
- [ ] ZADD key [NX|XX] [CH] [INCR] score member [score member ...]
- [ ] ZRANGE key start stop [WITHSCORES]
- [ ] ZREVRANGE key start stop [WITHSCORES]
- [ ] ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
- [ ] ZREM key member [member ...]
- [ ] ZCARD key
- [ ] ZSCORE key member