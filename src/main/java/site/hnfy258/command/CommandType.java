package site.hnfy258.command;

import java.util.function.Function;
import site.hnfy258.RedisCore;
import site.hnfy258.command.impl.*;
import site.hnfy258.command.impl.Hash.*;
import site.hnfy258.command.impl.List.*;
import site.hnfy258.command.impl.Set.*;
import site.hnfy258.command.impl.String.*;
import site.hnfy258.command.impl.Zset.*;

public enum CommandType {
    PING(core -> new Ping()),
    SET(Set::new),
    DEL(Del::new),
    INCR(Incr::new),
    MSET(Mset::new),
    EXIST(Exist::new),
    EXPIRE(Expire::new),
    SADD(Sadd::new),
    TTL(Ttl::new),
    SMEMBERS(Smemebers::new),
    SISMEMBER(Sismember::new),
    SCARD(Scard::new),
    SREM(Srem::new),
    SPOP(Spop::new),
    HSET(Hset::new),
    HGET(Hget::new),
    HMEST(Hmest::new),
    HGETALL(Hgetall::new),
    HMGET(Hmget::new),
    HDEL(Hdel::new),
    HEXISTS(Hexists::new),
    HLEN(Hlen::new),
    LPUSH(Lpush::new),
    RPUSH(Rpush::new),
    LRANGE(Lrange::new),
    LLEN(Llen::new),
    LPOP(Lpop::new),
    RPOP(Rpop::new),
    LREM(Lrem::new),
    ZADD(Zadd::new),
    ZRANGE(Zrange::new),
    ZREVRANGE(Zrevrange::new),
    ZRANGEBYSCORE(Zrangebyscore::new),
    ZREM(Zrem::new),
    ZCARD(Zcard::new),
    ZSCORE(Zscore::new),
    GET(Get::new);


    private final Function<RedisCore, Command> supplier;

    CommandType(Function<RedisCore, Command> supplier) {
        this.supplier = supplier;
    }

    public Function<RedisCore, Command> getSupplier() {
        return supplier;
    }
}