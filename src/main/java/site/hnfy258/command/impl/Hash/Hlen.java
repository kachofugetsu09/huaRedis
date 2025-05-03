package site.hnfy258.command.impl.Hash;

import site.hnfy258.RedisCore;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.datatype.RedisHash;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespInt;

public class Hlen implements Command {
    private final RedisCore redisCore;
    private BytesWrapper  key;

    public Hlen(RedisCore redisCore) {
        this.redisCore = redisCore;
    }

    @Override
    public CommandType getType() {
        return CommandType.HLEN;
    }

    @Override
    public void setContext(Resp[] array) {
        key = ((BulkString) array[1]).getContent();
    }

    @Override
    public Resp handle() {
        RedisHash redisHash = (RedisHash) redisCore.get(key);
        int count = redisHash.getMap().size();
        return new RespInt(count);
    }
}
