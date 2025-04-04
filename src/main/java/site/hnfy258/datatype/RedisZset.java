package site.hnfy258.datatype;

import site.hnfy258.utiils.SkipList;

public class RedisZset implements  RedisData{
    private volatile long timeout;
    private SkipList skipList = new SkipList();

    @Override
    public long timeout() {
        return timeout;
    }

    @Override
    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public boolean add(double score,String member){
        return skipList.insert(score,member);
    }
    public boolean remove(String member){
        return skipList.delete(member);
    }
    public Double getScore(String member){
        return skipList.getScore(member);
    }
}
