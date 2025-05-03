package site.hnfy258.command.impl.Zset;

import site.hnfy258.RedisCore;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.datatype.RedisData;
import site.hnfy258.datatype.RedisZset;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;
import site.hnfy258.protocal.RespInt;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Zrem implements Command {
    private final RedisCore redisCore;
    private BytesWrapper key;
    private List<String> members;

    public Zrem(RedisCore redisCore) {
        this.redisCore = redisCore;
    }

    @Override
    public CommandType getType() {
        return CommandType.ZREM;
    }

    @Override
    public void setContext(Resp[] array) {
        if(array.length < 3) {
            throw new IllegalArgumentException("Invalid ZREM command");
        }
        key = ((BulkString) array[1]).getContent();
        members = Stream.of(array).skip(2)
                .map(resp -> ((BulkString) resp).getContent().toUtf8String())
                .collect(Collectors.toList());
    }

    @Override
    public Resp handle() {
        RedisData redisData = redisCore.get(key);
        if(redisData == null) {
            return new RespInt(0);
        }
        if(!(redisData instanceof RedisZset)) {
            throw new IllegalArgumentException("Operation against a key holding the wrong kind of value");
        }

        RedisZset zset = (RedisZset) redisData;
        int count = 0;
        for(String member : members) {
            if(zset.remove(member)) {
                count++;
            }
        }
        return new RespInt(count);
    }
}
