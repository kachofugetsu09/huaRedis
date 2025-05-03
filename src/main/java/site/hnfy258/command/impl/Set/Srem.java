package site.hnfy258.command.impl.Set;

import site.hnfy258.RedisCore;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.datatype.RedisSet;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespInt;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Srem implements Command {
    private final RedisCore redisCore;
    private BytesWrapper key;
    private List<BytesWrapper> members;
    public Srem(RedisCore core) {
        this.redisCore = core;
    }

    @Override
    public CommandType getType() {
        return CommandType.SREM;
    }

    @Override
    public void setContext(Resp[] array) {
        key = ((BulkString) array[1]).getContent();
        members = Stream.of(array).skip(2).map(resp->
                ((BulkString) resp).getContent()).collect(Collectors.toList());
    }

    @Override
    public Resp handle() {
        RedisSet redisSet = (RedisSet) redisCore.get(key);
        int srem = redisSet.srem(members);
        return new RespInt(srem);
    }
}
