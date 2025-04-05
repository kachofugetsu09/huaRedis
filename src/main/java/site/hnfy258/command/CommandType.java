package site.hnfy258.command;

import java.util.function.Function;
import site.hnfy258.RedisCore;
import site.hnfy258.command.impl.*;
import site.hnfy258.command.impl.Set.*;
import site.hnfy258.command.impl.String.*;

public enum CommandType {
    PING(core -> new Ping()),
    SET(Set::new),
    DEL(Del::new),
    EXIST(Exist::new),
    EXPIRE(Expire::new),
    SADD(Sadd::new),
    TTL(Ttl::new),
    SMEMBERS(Smemebers::new),
    SISMEMBER(Sismember::new),
    SCARD(Scard::new),
    SREM(Srem::new),
    SPOP(Spop::new),
    GET(Get::new);


    private final Function<RedisCore, Command> supplier;

    CommandType(Function<RedisCore, Command> supplier) {
        this.supplier = supplier;
    }

    public Function<RedisCore, Command> getSupplier() {
        return supplier;
    }
}