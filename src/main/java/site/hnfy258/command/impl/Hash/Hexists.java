package site.hnfy258.command.impl.Hash;

import site.hnfy258.RedisCore;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.datatype.RedisHash;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Errors;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespInt;

public class Hexists implements Command {
    private final RedisCore redisCore;
    private BytesWrapper  key;
    private BytesWrapper field;

    public Hexists(RedisCore redisCore) {
        this.redisCore = redisCore;
    }

    @Override
    public CommandType getType() {
        return CommandType.HEXISTS;
    }

    @Override
    public void setContext(Resp[] array) {
        key = ((BulkString) array[1]).getContent();
        field = ((BulkString) array[2]).getContent();
    }

    @Override
    public Resp handle() {
        RedisHash redisHash = (RedisHash) redisCore.get(key);
        if(redisHash != null){
            return new RespInt(redisHash.getMap().containsKey(field) ? 1 : 0);
        }
        return new Errors("wrong type");
    }
}
