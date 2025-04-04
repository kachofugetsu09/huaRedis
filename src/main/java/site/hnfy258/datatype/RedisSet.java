package site.hnfy258.datatype;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

    public Collection<BytesWrapper> keys()
    {
        return redisSet;
    }
}
