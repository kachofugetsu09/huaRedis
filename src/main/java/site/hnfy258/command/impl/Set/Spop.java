package site.hnfy258.command.impl.Set;

import site.hnfy258.RedisCore;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.datatype.RedisData;
import site.hnfy258.datatype.RedisSet;
import site.hnfy258.protocal.*;

public class Spop implements Command {
    private RedisCore redisCore;
    private BytesWrapper key;
    int count = 1;
    public Spop(RedisCore redisCore) {
        this.redisCore = redisCore;
    }
    @Override
    public CommandType getType() {
        return CommandType.SPOP;
    }

    @Override
    public void setContext(Resp[] array) {
        key = ((BulkString) array[1]).getContent();
        if (array.length > 2) {
            try {
                String countStr = new String(((BulkString) array[2]).getContent().getBytes());
                count = Integer.parseInt(countStr);
            } catch (NumberFormatException e) {
                count = -1;
            }
        }
    }

    @Override
    public Resp handle() {
        RedisData redisData = redisCore.get(key);
        if (redisData == null) {
            return new Errors("no such set");
        }
        if (redisData instanceof RedisSet) {
            RedisSet redisSet = (RedisSet) redisData;
            return new RespArray(redisSet.spop(count).stream().map(BulkString::new).
                    toArray(Resp[]::new));
        }
        return new Errors("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
}
