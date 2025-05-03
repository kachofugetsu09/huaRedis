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
import site.hnfy258.protocal.SimpleString;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Zscore implements Command {
    private final RedisCore redisCore;
    private BytesWrapper key;
    private String members;

    public Zscore(RedisCore redisCore) {
        this.redisCore = redisCore;
    }

    @Override
    public CommandType getType() {
        return CommandType.ZSCORE;
    }

    @Override
    public void setContext(Resp[] array) {
        if (array.length < 2 || !(array[1] instanceof BulkString)) {
            throw new IllegalArgumentException("Invalid ZSCORE command");
        }
        key = ((BulkString) array[1]).getContent();
        members = ((BulkString) array[2]).getContent().toUtf8String();
    }

    @Override
    public Resp handle() {
        RedisData redisData = redisCore.get(key);
        if (redisData == null) {
            return new SimpleString("nil");
        }
        if (!(redisData instanceof RedisZset)) {
            throw new IllegalArgumentException("Operation against a key holding the wrong kind of value");
        }
        RedisZset redisZset = (RedisZset) redisData;
        Double score = redisZset.getScore(members);
        if (score == null) {
            return new SimpleString("nil");
        } else {
            return new BulkString(new BytesWrapper(score.toString().getBytes()));
        }

    }
}
