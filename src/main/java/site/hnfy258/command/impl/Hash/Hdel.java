package site.hnfy258.command.impl.Hash;

import site.hnfy258.RedisCore;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.datatype.RedisHash;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespInt;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Hdel implements Command {
    private RedisCore redisCore;
    private BytesWrapper  key;
    private List<BytesWrapper> fields;

    public Hdel(RedisCore redisCore) {
        this.redisCore = redisCore;
    }

    @Override
    public CommandType getType() {
        return CommandType.HDEL;
    }

    @Override
    public void setContext(Resp[] array) {
        key = ((BulkString) array[1]).getContent();
        fields = Stream.of(array).skip(2).map(resp->
                ((BulkString) resp).getContent()).collect(Collectors.toList());
    }

    @Override
    public Resp handle() {
        RedisHash redisHash = (RedisHash) redisCore.get(key);
        int del = redisHash.del(fields);
        return new RespInt(del);
    }
}
