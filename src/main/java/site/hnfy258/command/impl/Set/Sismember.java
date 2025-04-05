package site.hnfy258.command.impl.Set;

import site.hnfy258.RedisCore;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.datatype.RedisData;
import site.hnfy258.datatype.RedisSet;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Errors;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.SimpleString;

public class Sismember implements Command {
    private BytesWrapper key;
    private BytesWrapper member;
    private RedisCore redisCore;
    public Sismember(RedisCore redisCore){
        this.redisCore = redisCore;
    }

    @Override
    public CommandType getType() {
        return CommandType.SISMEMBER;
    }

    @Override
    public void setContext(Resp[] array) {
        key = ((BulkString) array[1]).getContent();
        member = ((BulkString) array[2]).getContent();
    }

    @Override
    public Resp handle() {
        RedisData redisData = redisCore.get(key);
        if(redisData == null){
            return new BulkString(new BytesWrapper("0".getBytes()));
        }
        if(redisData instanceof RedisSet){
            RedisSet redisSet = (RedisSet) redisData;
            boolean isMember = redisSet.keys().stream().anyMatch(bytesWrapper -> bytesWrapper.equals(member));
            return new BulkString(new BytesWrapper(isMember ? "1".getBytes() : "0".getBytes()));
        }
        return new Errors("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
}
