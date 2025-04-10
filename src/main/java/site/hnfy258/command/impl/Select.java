package site.hnfy258.command.impl;

import site.hnfy258.RedisCore;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.SimpleString;

public class Select implements Command {
    private RedisCore redisCore;
    private int index;

    public Select(RedisCore redisCore) {
        this.redisCore = redisCore;
    }

    @Override
    public CommandType getType() {
        return CommandType.SELECT;
    }

    @Override
    public void setContext(Resp[] array) {
        if (array.length < 2 || !(array[1] instanceof BulkString)) {
            throw new IllegalArgumentException("Invalid SELECT command");
        }
        index = Integer.parseInt(((BulkString) array[1]).getContent().toUtf8String());
    }

    @Override
    public Resp handle() {
        try {
            redisCore.selectDB(index);
            return new SimpleString("OK");
        } catch (IllegalArgumentException e) {
            return new SimpleString("ERR invalid DB index");
        }
    }
}
