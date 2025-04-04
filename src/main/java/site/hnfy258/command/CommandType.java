package site.hnfy258.command;

import java.util.function.Function;
import site.hnfy258.RedisCore;
import site.hnfy258.command.impl.Get;
import site.hnfy258.command.impl.Ping;
import site.hnfy258.command.impl.Set;

public enum CommandType {
    PING(core -> new Ping()),
    SET(core -> new Set(core)),
    GET(core -> new Get(core));

    private final Function<RedisCore, Command> supplier;

    CommandType(Function<RedisCore, Command> supplier) {
        this.supplier = supplier;
    }

    public Function<RedisCore, Command> getSupplier() {
        return supplier;
    }
}