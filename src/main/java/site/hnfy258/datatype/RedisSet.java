package site.hnfy258.datatype;

import java.util.*;

public class RedisSet implements  RedisData{
    private volatile long timeout = -1;
    private final Set<BytesWrapper> redisSet = new HashSet<>();
    @Override
    public long timeout() {
        return timeout;
    }

    @Override
    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public int sadd(List<BytesWrapper> members){
        return (int)members.stream().filter(redisSet::add).count();
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
}
