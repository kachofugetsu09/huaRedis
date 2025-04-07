package site.hnfy258.command.impl.String;

import site.hnfy258.RedisCore;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.datatype.RedisData;
import site.hnfy258.datatype.RedisString;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Errors;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespInt;

public class Incr implements Command {
    private RedisCore redisCore;
    private BytesWrapper key;

    public Incr(RedisCore redisCore) {
        this.redisCore = redisCore;
    }

    @Override
    public CommandType getType() {
        return CommandType.INCR;
    }

    @Override
    public void setContext(Resp[] array) {
        if (array.length != 2) {
            throw new IllegalArgumentException("INCR command requires exactly one key");
        }
        key = ((BulkString) array[1]).getContent();
    }

    @Override
    public Resp handle() {
        RedisData redisData = redisCore.get(key);
        if (redisData == null) {
            // If the key does not exist, create a new RedisString with value 1
            RedisString newString = new RedisString(new BytesWrapper("1".getBytes()));
            redisCore.put(key, newString);
            return new RespInt(1);
        }
        if (redisData instanceof RedisString) {
            RedisString redisString = (RedisString) redisData;
            try {
                long newValue = redisString.incr();
                return new RespInt((int) newValue);
            } catch (IllegalStateException e) {
                return new Errors("ERR value is not an integer or out of range");
            }
        }
        return new Errors("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
}
