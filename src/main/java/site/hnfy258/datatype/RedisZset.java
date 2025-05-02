package site.hnfy258.datatype;

import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;
import site.hnfy258.utils.SkipList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RedisZset implements  RedisData,Cloneable{
    private volatile long timeout;
    private SkipList skipList;

    public RedisZset() {
        this.timeout = -1;
        this.skipList = new SkipList();

    }

    @Override
    public long timeout() {
        return timeout;
    }

    @Override
    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    @Override
    public RedisData deepCopy() {
        try{
            RedisZset clone = (RedisZset) super.clone();
            clone.skipList = skipList.deepCopy();
            return clone;
        }catch(CloneNotSupportedException e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isImmutable() {
        return false;
    }

    @Override
    public List<Resp> convertToRESP() {
        List<Resp> res = new ArrayList<>();

        List<SkipList.Node> nodes = skipList.getRange(0, skipList.size()-1);

        for(SkipList.Node node:nodes){
            Resp[] resp = new Resp[2];
            resp[0] = new BulkString(new BytesWrapper(node.member.getBytes()));
            resp[1] = new BulkString(new BytesWrapper(String.valueOf(node.score).getBytes()));
            res.add(new RespArray(resp));
        }
        return res;
    }

    public boolean add(double score, String member) {
        return skipList.insert(score, member);
    }
    public boolean remove(String member){
        return skipList.delete(member);
    }
    public Double getScore(String member){
        return skipList.getScore(member);
    }

    public List<SkipList.Node> getRange(int start, int stop) {
        return skipList.getRange(start, stop);
    }

    public int size() {
        return skipList.size();
    }

    public List<SkipList.Node> getRangeByScore(int min, int max) {
        return skipList.getRangeByScore(min, max);
    }
}
