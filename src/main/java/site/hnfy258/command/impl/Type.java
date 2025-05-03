package site.hnfy258.command.impl;

import site.hnfy258.RedisCore;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datatype.*;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.SimpleString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.Errors;

public class Type implements Command {
    private BytesWrapper key;
    private final RedisCore redisCore;

    public Type(RedisCore core) {
        this.redisCore = core;
    }

    @Override
    public CommandType getType() {
        return CommandType.TYPE;
    }

    @Override
    public void setContext(Resp[] array) {
        if (array.length != 2) {
            throw new IllegalArgumentException("wrong number of arguments for 'type' command");
        }
        key = ((BulkString) array[1]).getContent();
    }

    @Override
    public Resp handle() {
        RedisData redisData = redisCore.get(key);
        if (redisData == null) {
            return new SimpleString("none");
        } else if (redisData instanceof RedisString) {
            return new SimpleString("string");
        } else if (redisData instanceof RedisList) {
            return new SimpleString("list");
        } else if (redisData instanceof RedisSet) {
            return new SimpleString("set");
        } else if (redisData instanceof RedisZset) {
            return new SimpleString("zset");
        } else if (redisData instanceof RedisHash) {
            return new SimpleString("hash");
        }
        return new SimpleString("none");
    }
}
