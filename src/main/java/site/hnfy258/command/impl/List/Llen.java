package site.hnfy258.command.impl.List;

import site.hnfy258.RedisCore;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.datatype.RedisData;
import site.hnfy258.datatype.RedisList;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Errors;
import site.hnfy258.protocal.Resp;

import java.util.List;

public class Llen implements Command {
    private BytesWrapper key;
    private final RedisCore redisCore;

    public Llen(RedisCore redisCore) {
        this.redisCore = redisCore;
    }

    @Override
    public CommandType getType() {
        return CommandType.LLEN;
    }

    @Override
    public void setContext(Resp[] array) {
        key = ((BulkString) array[1]).getContent();

    }

    @Override
    public Resp handle() {
        RedisData redisData = redisCore.get(key);
        if(redisData==null){
            return new BulkString(new BytesWrapper("0".getBytes()));
        }
        if(redisData instanceof RedisList){
            RedisList redisList = (RedisList) redisData;
            int size = redisList.size();
            return new BulkString(new BytesWrapper(String.valueOf(size).getBytes()));
        }
        return new Errors("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
}
