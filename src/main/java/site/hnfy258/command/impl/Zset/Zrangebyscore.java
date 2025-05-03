package site.hnfy258.command.impl.Zset;

import site.hnfy258.RedisCore;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.datatype.RedisData;
import site.hnfy258.datatype.RedisZset;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;
import site.hnfy258.protocal.RespInt;
import site.hnfy258.utils.SkipList;

import java.util.ArrayList;
import java.util.List;

public class Zrangebyscore implements Command {
    private final RedisCore redisCore;
    private BytesWrapper key;
    private int min;
    private int max;
    private boolean withScores;

    public Zrangebyscore(RedisCore redisCore) {
        this.redisCore = redisCore;
    }

    @Override
    public CommandType getType() {
        return CommandType.ZRANGEBYSCORE;
    }

    @Override
    public void setContext(Resp[] array) {
        key = ((BulkString) array[1]).getContent();
        min = Integer.parseInt(((BulkString) array[2]).getContent().toUtf8String());
        max = Integer.parseInt(((BulkString) array[3]).getContent().toUtf8String());
        if(array.length == 5){
            if(((BulkString) array[4]).getContent().toUtf8String().equalsIgnoreCase("WITHSCORES")){
                withScores = true;
            }
        }
    }

    @Override
    public Resp handle() {
        RedisData redisData = redisCore.get(key);
        if(redisData == null){
            return new RespInt(0);
        }
        if(min>max){
            return new RespInt(0);
        }
        if(redisData instanceof RedisZset){
            RedisZset redisZset = (RedisZset) redisData;
            List<SkipList.Node> list = redisZset.getRangeByScore(min, max);
            List<Resp> respList = new ArrayList<>();
            for(SkipList.Node node : list){
                respList.add(new BulkString(new BytesWrapper(node.member.getBytes())));
                if(withScores){
                    respList.add(new BulkString(new BytesWrapper(String.valueOf(node.score).getBytes())));
                }
            }
            return new RespArray(respList.toArray(new Resp[0]));
        }
        return new RespArray(new Resp[0]);
    }
}
