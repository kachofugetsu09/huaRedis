package site.hnfy258.command.impl.Hash;

import site.hnfy258.RedisCore;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.datatype.RedisData;
import site.hnfy258.datatype.RedisHash;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Errors;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespInt;

public class Hset implements Command {
    private RedisCore redisCore;
    private BytesWrapper  key;
    private BytesWrapper field;
    private BytesWrapper value;

    public Hset(RedisCore redisCore) {
        this.redisCore = redisCore;
    }

    @Override
    public CommandType getType() {
        return CommandType.HSET;
    }

    @Override
    public void setContext(Resp[] array) {
        key = ((BulkString) array[1]).getContent();
        field = ((BulkString) array[2]).getContent();
        value = ((BulkString) array[3]).getContent();
    }

    @Override
    public Resp handle() {
        RedisData redisData = redisCore.get(key);
        if (redisData == null) {
            RedisHash redisHash = new RedisHash();
            int put = redisHash.put(field, value);
            redisCore.put(key, redisHash);
            return new RespInt(put);
        } else if (redisData instanceof RedisHash) {
            RedisHash redisHash = (RedisHash) redisData;
            int put = redisHash.put(field, value);
            return new RespInt(put);
        } else {
            return new Errors("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
    }
}
