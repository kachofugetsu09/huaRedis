package site.hnfy258.datatype;

import site.hnfy258.utils.SkipList;

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
