package site.hnfy258.command.impl.Hash;

import site.hnfy258.RedisCore;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.datatype.RedisData;
import site.hnfy258.datatype.RedisHash;
import site.hnfy258.protocal.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Hmget implements Command {
    private RedisCore redisCore;
    private BytesWrapper  key;
    private List<BytesWrapper> fields;

    public Hmget(RedisCore redisCore) {
        this.redisCore = redisCore;
    }

    @Override
    public CommandType getType() {
        return CommandType.HMGET;
    }

    @Override
    public void setContext(Resp[] array) {
        key = ((BulkString) array[1]).getContent();
        fields = Stream.of(array).skip(2).map(resp-> ((BulkString) resp).getContent()).collect(Collectors.toList());
    }

    @Override
    public Resp handle() {
        RedisData redisData = redisCore.get(key);
        for(BytesWrapper field:fields){
            if(redisData == null){
                return new Errors("WRONGTYPE Operation against a key holding the wrong kind of value");
            }else if(redisData instanceof RedisHash){
                RedisHash redisHash = (RedisHash) redisData;
                Map<BytesWrapper, BytesWrapper> map = redisHash.getMap();
                Resp[] array = new Resp[fields.size()];
                for(int i=0;i<fields.size();i++){
                    if(map.containsKey(fields.get(i))){
                        array[i] = new BulkString(map.get(fields.get(i)));
                    }else{
                        array[i] = new BulkString(new BytesWrapper(("(nil)").getBytes()));
                    }
                }
                return new RespArray(array);

            }else{
                return new Errors("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
        }
        return new Errors("ERR no such key");
    }

}
