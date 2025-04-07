package site.hnfy258.command.impl.String;

import site.hnfy258.RedisCore;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.datatype.RedisString;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.SimpleString;

import java.util.ArrayList;
import java.util.List;

public class Mset implements Command {
    private RedisCore core;
    private List<BytesWrapper> keys;
    private List<BytesWrapper> values;

    public Mset(RedisCore core) {
        this.core = core;
        this.keys = new ArrayList<>();
        this.values = new ArrayList<>();
    }

    @Override
    public CommandType getType() {
        return CommandType.MSET;
    }

    @Override
    public void setContext(Resp[] array) {
        if (array.length < 3 || (array.length - 1) % 2 != 0) {
            throw new IllegalArgumentException("Invalid MSET command");
        }

        for (int i = 1; i < array.length; i += 2) {
            if (!(array[i] instanceof BulkString) || !(array[i + 1] instanceof BulkString)) {
                throw new IllegalArgumentException("Invalid MSET command");
            }
            keys.add(((BulkString) array[i]).getContent());
            values.add(((BulkString) array[i + 1]).getContent());
        }
    }

    @Override
    public Resp handle() {
        for (int i = 0; i < keys.size(); i++) {
            BytesWrapper key = keys.get(i);
            BytesWrapper value = values.get(i);

            RedisString stringData = new RedisString(value);
            core.put(key, stringData);
        }

        return new SimpleString("OK");
    }
}