package site.hnfy258.command.impl.String;

import site.hnfy258.RedisCore;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.datatype.RedisData;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespInt;

public class Expire implements Command {
    private RedisCore redisCore;
    private BytesWrapper key;
    private int second;


    public Expire(RedisCore redisCore) {
        this.redisCore = redisCore;
    }

    @Override
    public CommandType getType() {
        return CommandType.EXPIRE;
    }

    @Override
    public void setContext(Resp[] array) {
        key = ((BulkString) array[1]).getContent();
        second = Integer.parseInt(((BulkString)array[2]).getContent().toUtf8String());
    }

    @Override
    public Resp handle() {
        RedisData redisData = redisCore.get(key);
        if(redisData == null){
            return new RespInt(0);
        }
        redisData.setTimeout(System.currentTimeMillis() + second * 1000L);
        return new RespInt(1);
    }
}
