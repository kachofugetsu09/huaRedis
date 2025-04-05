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
import site.hnfy258.protocal.RespInt;

public class Scard implements Command {
    private final RedisCore redisCore;
    private BytesWrapper key;

    public Scard(RedisCore core) {
        this.redisCore = core;
    }

    @Override
    public CommandType getType() {
        return CommandType.SCARD;
    }

    @Override
    public void setContext(Resp[] array) {
        key = ((BulkString) array[1]).getContent();
    }

    @Override
    public Resp handle() {
        RedisData redisData = redisCore.get(key);
        if(redisData == null){
            return new RespInt(0);
        }
        if(redisData instanceof RedisSet){
            RedisSet redisSet = (RedisSet) redisData;
            return new RespInt(redisSet.keys().size());
        }
        return new Errors("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
}
