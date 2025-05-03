package site.hnfy258.command.impl.List;

import org.apache.log4j.Logger;
import site.hnfy258.RedisCore;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.datatype.RedisData;
import site.hnfy258.datatype.RedisList;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Errors;
import site.hnfy258.protocal.Resp;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Lpush implements Command {
    private BytesWrapper key;
    private List<BytesWrapper> element;
    private final RedisCore redisCore;

    Logger logger = Logger.getLogger(Lpush.class);

    public Lpush(RedisCore redisCore) {
        this.redisCore = redisCore;
    }

    @Override
    public CommandType getType() {
        return CommandType.LPUSH;
    }

    @Override
    public void setContext(Resp[] array) {
        if (array.length < 3) {
            throw new IllegalArgumentException("LPUSH command requires at least one value");
        }
        key = ((BulkString) array[1]).getContent();
        element = Stream.of(array).skip(2)
                .map(resp -> ((BulkString) resp).getContent())
                .collect(Collectors.toList());
    }

    @Override
    public Resp handle() {
        RedisData data = redisCore.get(key);
        RedisList list;

        if (data == null) {
            list = new RedisList();
        } else if (data instanceof RedisList) {
            list = (RedisList) data;
        } else {
            return new Errors("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        BytesWrapper[] elementsArray = element.toArray(new BytesWrapper[0]);
        list.lpush(elementsArray);

        redisCore.put(key, list);

        int size = list.size();
        //logger.info("LPUSH: " + size);
        return new BulkString(new BytesWrapper(String.valueOf(size).getBytes()));
    }
}