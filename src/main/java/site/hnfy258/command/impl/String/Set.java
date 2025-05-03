// Set.java
package site.hnfy258.command.impl.String;

import site.hnfy258.RedisCore;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.datatype.RedisString;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.SimpleString;

public class Set implements Command {
    private final RedisCore redisCore;
    private BytesWrapper key;
    private BytesWrapper value;
    private long timeout = -1;
    private boolean notExistSet = false;
    private boolean existSet = false;

    public Set(RedisCore core) {
        this.redisCore = core;
    }

    @Override
    public CommandType getType() {
        return CommandType.SET;
    }

    @Override
    public void setContext(Resp[] array) {
        key = ((BulkString) array[1]).getContent();
        value = ((BulkString) array[2]).getContent();
        int index = 3;
        while (index < array.length) {
            String string = ((BulkString) array[index]).getContent().toUtf8String();
            index++;
            if (string.startsWith("EX")) {
                String second = ((BulkString) array[index]).getContent().toUtf8String();
                timeout = Long.parseLong(second) * 1000;
            } else if (string.startsWith("PX")) {
                String millisecond = ((BulkString) array[index]).getContent().toUtf8String();
                timeout = Long.parseLong(millisecond);
            } else if (string.startsWith("NX")) {
                notExistSet = true;
            } else if (string.startsWith("XX")) {
                existSet = true;
            }
        }
    }

    @Override
    public Resp handle() {
        if (notExistSet && redisCore.exist(key)) {
            return BulkString.NullBulkString;
        } else if (existSet && !redisCore.exist(key)) {
            return BulkString.NullBulkString;
        } else {
            if (timeout != -1) {
                timeout += System.currentTimeMillis();
            }
            RedisString stringData = new RedisString(value);
            stringData.setTimeout(timeout);
            redisCore.put(key, stringData);
            return new SimpleString("OK");
        }
    }
}
