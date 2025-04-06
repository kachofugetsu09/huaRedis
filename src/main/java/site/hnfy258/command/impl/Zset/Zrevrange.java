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
import site.hnfy258.utiils.SkipList;

import java.util.ArrayList;
import java.util.List;

public class Zrevrange implements Command {
    private RedisCore redisCore;
    private BytesWrapper key;
    private int start;
    private int stop;
    private boolean withScores;

    public Zrevrange(RedisCore redisCore)
    {
        this.redisCore = redisCore;
    }

    @Override
    public CommandType getType() {
        return CommandType.ZREVRANGE;
    }

    @Override
    public void setContext(Resp[] array) {
        key = ((BulkString) array[1]).getContent();
        start = Integer.parseInt(((BulkString) array[2]).getContent().toUtf8String());
        stop = Integer.parseInt(((BulkString) array[3]).getContent().toUtf8String());
        if(array.length == 5){
            if(((BulkString) array[4]).getContent().toUtf8String().equalsIgnoreCase("WITHSCORES")){
                withScores = true;
            }
        }
    }

    @Override
    public Resp handle() {
        RedisData redisData = redisCore.get(key);
        if (!(redisData instanceof RedisZset)) {
            return new RespArray(new Resp[0]);
        }

        RedisZset redisZset = (RedisZset) redisData;
        int size = redisZset.size();

        // 处理负索引
        if (start < 0) start = size + start;
        if (stop < 0) stop = size + stop;

        // 确保索引在有效范围内
        start = Math.max(start, 0);
        stop = Math.min(stop, size - 1);

        // 如果开始索引大于结束索引，返回空列表
        if (start > stop) {
            return new RespArray(new Resp[0]);
        }

        // 反转索引以获取反向范围
        int revStart = size - stop - 1;
        int revStop = size - start - 1;

        List<SkipList.Node> range = redisZset.getRange(revStart, revStop);

        List<Resp> respList = new ArrayList<>();
        for (int i = range.size() - 1; i >= 0; i--) {
            SkipList.Node node = range.get(i);
            respList.add(new BulkString(new BytesWrapper(node.member.getBytes())));
            if (withScores) {
                respList.add(new BulkString(new BytesWrapper(String.valueOf(node.score).getBytes())));
            }
        }
        return new RespArray(respList.toArray(new Resp[0]));
    }
}
