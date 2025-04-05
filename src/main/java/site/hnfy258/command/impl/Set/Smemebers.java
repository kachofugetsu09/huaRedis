package site.hnfy258.command.impl.Set;

import site.hnfy258.RedisCore;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.datatype.RedisData;
import site.hnfy258.datatype.RedisSet;
import site.hnfy258.protocal.*;

import java.util.Collection;
import java.util.stream.Collectors;

public class Smemebers implements Command {
    private BytesWrapper key;
    private RedisCore redisCore;

    public Smemebers(RedisCore core) {
        this.redisCore = core;
    }

    @Override
    public CommandType getType() {
        return CommandType.SMEMBERS;
    }

    @Override
    public void setContext(Resp[] array) {
        key = ((BulkString) array[1]).getContent();
    }

    @Override
    public Resp handle() {
        RedisData redisData = redisCore.get(key);
        if(redisData == null){
            return new Errors("no such set");
        }
        if(redisData instanceof RedisSet){
            RedisSet redisSet = (RedisSet) redisData;
            Collection<BytesWrapper> keys = redisSet.keys();
            Resp[] bulkStrings = keys.stream()
                    .map(BulkString::new)
                    .toArray(Resp[]::new);
            return new RespArray(bulkStrings);
        }
        return new Errors("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
}
