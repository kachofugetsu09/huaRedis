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
import site.hnfy258.protocal.SimpleString;

import java.util.HashMap;
import java.util.Map;

public class Hmest implements Command {
    private final RedisCore redisCore;
    private BytesWrapper  key;
    private Map<BytesWrapper, BytesWrapper> fields;

    public Hmest(RedisCore redisCore) {
        this.redisCore = redisCore;
    }

    @Override
    public CommandType getType() {
        return CommandType.HMEST;
    }

    @Override
    public void setContext(Resp[] array) {
        key = ((BulkString) array[1]).getContent();

        if((array.length-2)%2!=0){
            throw new RuntimeException("参数错误");
        }
        fields = new HashMap<>();
        for(int i=2;i<array.length;i+=2){
            BytesWrapper field = ((BulkString) array[i]).getContent();
            BytesWrapper value = ((BulkString) array[i+1]).getContent();
            fields.put(field,value);
        }
    }

    @Override
    public Resp handle() {
        RedisData redisData = redisCore.get(key);
        if(redisData==null){
            RedisHash redisHash = new RedisHash();
            for (Map.Entry<BytesWrapper, BytesWrapper> entry : fields.entrySet()) {
                redisHash.put(entry.getKey(), entry.getValue());
            }
            redisCore.put(key, redisHash);
            return new SimpleString("OK");
        }
        if (!(redisData instanceof RedisHash)) {
            return new Errors("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        RedisHash redisHash = (RedisHash) redisData;
        for (Map.Entry<BytesWrapper, BytesWrapper> entry : fields.entrySet()) {
            redisHash.put(entry.getKey(), entry.getValue());
        }
        return new SimpleString("OK");
    }
}
