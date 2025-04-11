package site.hnfy258.datatype;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RedisHash implements  RedisData,Cloneable{
    private volatile long timeout = -1;
    private final Map<BytesWrapper, BytesWrapper> redisHashMap = new ConcurrentHashMap<>();
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
        try {
            RedisHash clone = (RedisHash) super.clone();
            clone.redisHashMap.putAll(redisHashMap);
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }


    public int put(BytesWrapper field,BytesWrapper value){
        return redisHashMap.put(field,value)==null?1:0;
    }

    public Map<BytesWrapper,BytesWrapper> getMap() {
        return redisHashMap;
    }

    public int del(List<BytesWrapper> fields){
        return (int)fields.stream().
                filter(key-> redisHashMap.remove(key)!=null).count();
    }
}
