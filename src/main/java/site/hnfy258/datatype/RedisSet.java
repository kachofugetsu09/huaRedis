package site.hnfy258.datatype;

import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;

public class RedisSet implements  RedisData,Cloneable{
    private volatile long timeout = -1;
    private final Set<BytesWrapper> redisSet = new ConcurrentSkipListSet<>();
    Logger logger = Logger.getLogger(RedisSet.class);
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
            RedisSet clone = (RedisSet) super.clone();
            clone.redisSet.addAll(redisSet);
            return clone;
        }catch(CloneNotSupportedException e){
            throw new RuntimeException(e);
        }
    }

    public int sadd(List<BytesWrapper> members){
        //logger.info("Adding members to set: " + members);
        int addedCount = (int)members.stream().filter(redisSet::add).count();
        //logger.info("Added " + addedCount + " members to set");
        return addedCount;
    }

    public int srem(List<BytesWrapper> members){
        return (int)members.stream().filter(redisSet::remove).count();
    }

    public List<BytesWrapper> spop(int count){
        Random random = new Random();
        List<BytesWrapper> result = new ArrayList<>();
        while(count>0 && !redisSet.isEmpty()){
            int index = random.nextInt(redisSet.size());
            Iterator<BytesWrapper> iterator = redisSet.iterator();
            for (int i = 0; i < index; i++) {
                iterator.next();
            }
            result.add(iterator.next());
            iterator.remove();
            count--;
        }
        return result;
    }

    public Collection<BytesWrapper> keys()
    {
        return redisSet;
    }

    public int size() {
        return redisSet.size();
    }
}
