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

public class Hget implements Command {
    private RedisCore redisCore;
    private BytesWrapper  key;
    private BytesWrapper field;

    public Hget(RedisCore redisCore) {
        this.redisCore = redisCore;
    }

    @Override
    public CommandType getType() {
        return CommandType.HGET;
    }

    @Override
    public void setContext(Resp[] array) {
        key = ((BulkString) array[1]).getContent();
        field = ((BulkString) array[2]).getContent();
    }

    @Override
    public Resp handle() {
        RedisData redisData = redisCore.get(key);
        if(redisData instanceof RedisHash){
            RedisHash redisHash = (RedisHash) redisData;
            return new BulkString(redisHash.getMap().get(field));
        }
        return new Errors("类型错误");
    }
}
