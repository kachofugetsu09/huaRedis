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

import java.util.Collections;

public class Lpop implements Command {
    private BytesWrapper key;
    private RedisCore redisCore;

    public Lpop(RedisCore redisCore) {
        this.redisCore = redisCore;
    }

    @Override
    public CommandType getType() {
        return CommandType.LPOP;
    }

    @Override
    public void setContext(Resp[] array) {
        if (array.length < 2) {
            throw new IllegalArgumentException("LPOP command requires a key");
        }
        key = ((BulkString) array[1]).getContent();
    }

    @Override
    public Resp handle() {
        RedisData data = redisCore.get(key);

        if (data == null) {
            return new BulkString((BytesWrapper) null );
        }

        if (!(data instanceof RedisList)) {
            return new Errors("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        RedisList redisList = (RedisList) data;
        BytesWrapper value = redisList.lpop();

        if (redisList.size() == 0) {
            redisCore.remove(Collections.singletonList(key));
        } else {
            redisCore.put(key, redisList);
        }

        return new BulkString(value);
    }
}