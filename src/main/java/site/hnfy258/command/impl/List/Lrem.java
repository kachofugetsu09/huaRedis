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
import site.hnfy258.protocal.RespInt;

import java.util.Collections;

public class Lrem implements Command {
    private BytesWrapper key;
    private BytesWrapper value;
    private RedisCore redisCore;

    public Lrem(RedisCore redisCore) {
        this.redisCore = redisCore;
    }

    @Override
    public CommandType getType() {
        return CommandType.LREM;
    }

    @Override
    public void setContext(Resp[] array) {
        key = ((BulkString) array[1]).getContent();
        value = ((BulkString) array[2]).getContent();
    }

    @Override
    public Resp handle() {
        RedisData redisData = redisCore.get(key);
        if(redisData == null){
            return new Errors("no such key");
        }
        if(redisData instanceof RedisList){
            RedisList redisList = (RedisList) redisData;
            int remove = redisList.remove(value);
            return new RespInt(remove);
        }
        return new Errors("illegal operation");
    }
}