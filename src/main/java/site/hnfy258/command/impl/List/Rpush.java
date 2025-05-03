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
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Rpush implements Command {
    private BytesWrapper key;
    private List<BytesWrapper> element;
    private final RedisCore redisCore;

    public Rpush(RedisCore redisCore) {
        this.redisCore = redisCore;
    }

    @Override
    public CommandType getType() {
        return CommandType.RPUSH;
    }

    @Override
    public void setContext(Resp[] array) {
        if (array.length < 3) {
            throw new IllegalArgumentException("RPUSH command requires at least one value");
        }
        key = ((BulkString) array[1]).getContent();
        element = Stream.of(array).skip(2)
                .map(resp -> ((BulkString) resp).getContent())
                .collect(Collectors.toList());

    }

    @Override
    public Resp handle() {
        RedisData redisData = redisCore.get(key);
        RedisList redisList;
        if(redisData==null){
            redisList = new RedisList();
        }
        else if(redisData instanceof RedisList){
            redisList =(RedisList) redisData;
        }
        else{
            return new Errors("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        BytesWrapper[] array = element.toArray(new BytesWrapper[0]);
        redisList.rpush(array);
        redisCore.put(key,redisList);
        int size = redisList.size();
        return new BulkString(new BytesWrapper(String.valueOf(size).getBytes()));
    }
}
