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
import site.hnfy258.protocal.RespArray;

import java.util.List;
import java.util.stream.Collectors;

public class Lrange implements Command {
    private BytesWrapper key;
    private RedisCore redisCore;
    private int start;
    private int end;

    public Lrange(RedisCore redisCore) {
        this.redisCore = redisCore;
    }

    @Override
    public CommandType getType() {
        return CommandType.LRANGE;
    }

    @Override
    public void setContext(Resp[] array) {
        if (array.length < 4) {
            throw new IllegalArgumentException("LRANGE command requires at least one value");
        }
        key = ((BulkString) array[1]).getContent();
        start = Integer.parseInt(((BulkString) array[2]).getContent().toUtf8String());
        end = Integer.parseInt(((BulkString) array[3]).getContent().toUtf8String());
    }

    @Override
    public Resp handle() {
        RedisData redisData = redisCore.get(key);
        if (redisData == null) {
            return new BulkString(new BytesWrapper("".getBytes()));
        }
        if (redisData instanceof RedisList) {
            RedisList redisList = (RedisList) redisData;
            List<BytesWrapper> lrange = redisList.lrange(start, end);

            // 直接创建RespArray，避免中间集合
            Resp[] respArray = new Resp[lrange.size()];
            for (int i = 0; i < lrange.size(); i++) {
                respArray[i] = new BulkString(lrange.get(i));
            }

            return new RespArray(respArray);
        }
        return new Errors("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
}
