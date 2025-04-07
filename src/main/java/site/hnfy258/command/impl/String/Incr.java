package site.hnfy258.command.impl.String;

import site.hnfy258.RedisCore;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;

public class Incr implements Command {
    private RedisCore redisCore;
    private BytesWrapper key;

    public Incr(RedisCore redisCore) {
        this.redisCore = redisCore;
    }
    @Override
    public CommandType getType() {
        return CommandType.INCR;
    }

    @Override
    public void setContext(Resp[] array) {
        key = ((BulkString) array[1]).getContent();
    }

    @Override
    public Resp handle() {
        return null;
    }
}
