package site.hnfy258.command.impl.String;

import site.hnfy258.RedisCore;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespInt;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Del implements Command {
    private RedisCore redisCore;
    private List<BytesWrapper> keys;

    public Del(RedisCore redisCore) {
        this.redisCore = redisCore;
    }
    @Override
    public CommandType getType() {
        return CommandType.DEL;
    }

    @Override
    public void setContext(Resp[] array) {
        keys = Stream.of(array).skip(1).map(resp->
                ((BulkString) resp).getContent()).collect(Collectors.toList());
    }

    @Override
    public Resp handle() {
        long remove = redisCore.remove(keys);
        return new RespInt((int) remove);
    }
}
