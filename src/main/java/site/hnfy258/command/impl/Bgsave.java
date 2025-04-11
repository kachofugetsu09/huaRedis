package site.hnfy258.command.impl;

import site.hnfy258.RedisCore;
import site.hnfy258.RedisCoreImpl;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.SimpleString;
import site.hnfy258.rdb.core.RDBHandler;

public class Bgsave implements Command {
    private final RedisCore redisCore;
    private final RDBHandler rdbHandler;

    public Bgsave(RedisCore redisCore) {
        this.redisCore = redisCore;
        this.rdbHandler = ((RedisCoreImpl) redisCore).getRDBHandler();
    }

    @Override
    public CommandType getType() {
        return CommandType.BGSAVE;
    }

    @Override
    public void setContext(Resp[] array) {
        // BGSAVE 命令不需要额外参数,所以这里不需要做任何事
    }

    @Override
    public Resp handle() {
        if (rdbHandler == null) {
            return new SimpleString("ERR RDB persistence is not enabled");
        }

        // 检查是否已经在进行保存
        if (rdbHandler.isSaving()) {
            return new SimpleString("ERR Background save already in progress");
        }

        // 触发后台全量保存
        boolean started = rdbHandler.bgsave(true);
        if (started) {
            return new SimpleString("Background saving started");
        } else {
            return new SimpleString("ERR Background save could not be started");
        }
    }
}