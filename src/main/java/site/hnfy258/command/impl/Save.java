package site.hnfy258.command.impl;

import site.hnfy258.RedisCore;
import site.hnfy258.RedisCoreImpl;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.SimpleString;
import site.hnfy258.rdb.RDBHandler;

public class Save implements Command {
    private final RedisCore redisCore;
    private final RDBHandler rdbHandler;

    public Save(RedisCore redisCore) {
        this.redisCore = redisCore;
        this.rdbHandler = ((RedisCoreImpl) redisCore).getRDBHandler();
    }

    @Override
    public CommandType getType() {
        return CommandType.SAVE;
    }

    @Override
    public void setContext(Resp[] array) {
        // SAVE 命令不需要额外参数,所以这里不需要做任何事
    }

    @Override
    public Resp handle() {
        if (rdbHandler == null) {
            return new SimpleString("ERR RDB persistence is not enabled");
        }

        try {
            rdbHandler.save();
            return new SimpleString("OK");
        } catch (Exception e) {
            return new SimpleString("ERR " + e.getMessage());
        }
    }
}
