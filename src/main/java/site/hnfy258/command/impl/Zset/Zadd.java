package site.hnfy258.command.impl.Zset;

import site.hnfy258.RedisCore;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.datatype.RedisZset;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespInt;

import java.util.ArrayList;
import java.util.List;

public class Zadd implements Command {
    private RedisCore redisCore;
    private BytesWrapper key;
    private List<Double> scores;
    private List<String> members;

    public Zadd(RedisCore redisCore){
        this.redisCore = redisCore;
    }

    @Override
    public CommandType getType() {
        return CommandType.ZADD;
    }

    @Override
    public void setContext(Resp[] array) {
        key = ((BulkString) array[1]).getContent();
        scores = new ArrayList<>();
        members = new ArrayList<>();
        for (int i = 2; i < array.length; i += 2) {
            scores.add(Double.parseDouble((((BulkString) array[i]).getContent()).toUtf8String()));
            members.add((((BulkString) array[i+1]).getContent()).toUtf8String());
        }
    }

    @Override
    public Resp handle() {
        RedisZset zset = (RedisZset) redisCore.get(key);
        if (zset == null) {
            System.out.println("zset is null");
            zset = new RedisZset();
            redisCore.put(key, zset);
        }

        int addedOrUpdated = 0;
        for (int i = 0; i < scores.size(); i++) {
            double score = scores.get(i);
            String member = members.get(i);
            if (zset.add(score, member)) {
                addedOrUpdated++;
            }
        }

        return new RespInt(addedOrUpdated);
    }
}