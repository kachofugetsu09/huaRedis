package site.hnfy258.command.impl.Zset;

import site.hnfy258.RedisCore;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.datatype.RedisData;
import site.hnfy258.datatype.RedisZset;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespInt;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Zcard implements Command {
    private final RedisCore redisCore;
    private BytesWrapper key;

    public Zcard(RedisCore redisCore) {
        this.redisCore = redisCore;
    }

    @Override
    public CommandType getType() {
        return CommandType.ZCARD;
    }

    @Override
    public void setContext(Resp[] array) {
        if (array.length < 2 || !(array[1] instanceof BulkString)) {
            throw new IllegalArgumentException("Invalid ZCARD command");
        }
        key = ((BulkString) array[1]).getContent();
    }

    @Override
    public Resp handle() {
        RedisData redisData = redisCore.get(key);
        if(redisData == null){
            return new RespInt(0);
        }
        if(redisData instanceof RedisZset){
            RedisZset redisZset = (RedisZset) redisData;
            return new RespInt(redisZset.size());
        }
        return new RespInt(0);
    }

}
