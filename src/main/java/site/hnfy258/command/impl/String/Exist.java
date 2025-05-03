package site.hnfy258.command.impl.String;

import site.hnfy258.RedisCore;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespInt;

public class Exist implements Command {
    private BytesWrapper key;
    private final RedisCore redisCore;

    public Exist(RedisCore redisCore) {
        this.redisCore = redisCore;
    }

    @Override
    public CommandType getType() {
        return CommandType.EXIST;
    }

    @Override
    public void setContext(Resp[] array) {
        key = ((BulkString) array[1]).getContent();
    }

    @Override
    public Resp handle() {
        boolean exist = redisCore.exist(key);
        if(exist){
            return new RespInt(1);
        }
        return new RespInt(0);
    }
}
