package site.hnfy258.command.impl;

import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.datatype.RedisString;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.RedisCore;

public class Get implements Command {
    private BytesWrapper key;
    private RedisCore redisCore;

    public Get(RedisCore redisCore) {
        this.redisCore = redisCore;
    }

    @Override
    public CommandType getType() {
        return CommandType.GET;
    }

    @Override
    public void setContext(Resp[] array) {
        if (array.length < 2 || !(array[1] instanceof BulkString)) {
            throw new IllegalArgumentException("Invalid GET command");
        }
        this.key = ((BulkString) array[1]).getContent();
    }

    @Override
    public Resp handle() {
        RedisString value = (RedisString) redisCore.get(key);
        if (value == null) {
            return BulkString.NullBulkString;
        }
        return new BulkString(value.getValue());
    }
}