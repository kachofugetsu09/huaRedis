package site.hnfy258.command.impl.String;

import site.hnfy258.RedisCore;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.datatype.RedisData;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespInt;

public class Ttl implements Command {
    private RedisCore redisCore;
    private BytesWrapper key;

    public Ttl(RedisCore core) {
        this.redisCore = core;
    }

    @Override
    public CommandType getType() {
        return CommandType.TTL;
    }

    @Override
    public void setContext(Resp[] array) {
        key = ((BulkString) array[1]).getContent();
    }

    @Override
    public Resp handle() {
        RedisData redisData = redisCore.get(key);
        if(redisData == null){
            return new RespInt(-2);
        }
        else if(redisData.timeout() == -1){
            return new RespInt(-1);
        }
        else{
            long second = (redisData.timeout() - System.currentTimeMillis()) / 1000;
            return new RespInt((int) second);
        }
    }
}
