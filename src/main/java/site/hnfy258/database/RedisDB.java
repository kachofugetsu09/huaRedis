package site.hnfy258.database;

import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.datatype.RedisData;

import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class RedisDB {
    private final ConcurrentHashMap<BytesWrapper, RedisData> data;
    private final int id;

    public RedisDB(int id) {
        this.id = id;
        this.data = new ConcurrentHashMap<>();
    }

    public int getId() {
        return id;
    }

    public Set<BytesWrapper> keys() {
        return data.keySet();
    }

    public boolean exist(BytesWrapper key) {
        return data.containsKey(key);
    }

    public void put(BytesWrapper key, RedisData redisData) {
        data.put(key, redisData);
    }

    public RedisData get(BytesWrapper key) {
        RedisData redisData = data.get(key);
        if (redisData == null) {
            return null;
        }
        if (redisData.timeout() == -1) {
            return redisData;
        }
        if (redisData.timeout() < System.currentTimeMillis()) {
            data.remove(key);
            return null;
        }
        return redisData;
    }

    public long remove(List<BytesWrapper> keys) {
        return keys.stream().peek(data::remove).count();
    }

    public void clear() {
        data.clear();
    }

    public int size() {
        return data.size();
    }

    public Map<BytesWrapper, RedisData> getAll() {
        return data;
    }
}
