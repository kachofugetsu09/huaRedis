package site.hnfy258.datatype;

import site.hnfy258.utiils.SkipList;

import java.util.List;

public class RedisZset implements  RedisData{
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
