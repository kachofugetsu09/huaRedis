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
import site.hnfy258.protocal.RespArray;

import java.util.Map;

public class Hgetall implements Command {
    private RedisCore redisCore;
    private BytesWrapper  key;

    public Hgetall(RedisCore redisCore) {
        this.redisCore = redisCore;
    }

    @Override
    public CommandType getType() {
        return CommandType.HGETALL;
    }

    @Override
    public void setContext(Resp[] array) {
        key = ((BulkString) array[1]).getContent();
    }

    @Override
    public Resp handle() {
        RedisData redisData = redisCore.get(key);
        if(redisData instanceof RedisHash){
            RedisHash redisHash = (RedisHash) redisData;
            Map<BytesWrapper,BytesWrapper> map = redisHash.getMap();
            Resp[] array = new Resp[map.size()*2];
            int i=0;
            for(BytesWrapper field : map.keySet()){
                array[i] = new BulkString(field);
                array[i+1] = new BulkString(map.get(field));
                i+=2;
            }
            return new RespArray(array);

        }
        return new Errors("ERR no such key");
    }
}
