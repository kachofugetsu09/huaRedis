package site.hnfy258.rdb.policy;

import site.hnfy258.datatype.BytesWrapper;
import java.util.*;
import java.util.concurrent.*;

public class ChangeTracker {
    private final Map<Integer, Map<BytesWrapper, Long>> lastModifiedMap = new ConcurrentHashMap<>();

    public void trackChange(int dbIndex, BytesWrapper key) {
        lastModifiedMap.computeIfAbsent(dbIndex, k -> new ConcurrentHashMap<>())
                     .put(key, System.currentTimeMillis());
    }

    public Map<Integer, Map<BytesWrapper, Long>> getModifiedData() {
        return new HashMap<>(lastModifiedMap);
    }

    public boolean hasModifications() {
        return !lastModifiedMap.isEmpty() && 
               lastModifiedMap.values().stream()
                              .mapToInt(Map::size).sum() > 100;
    }

    public void clear() {
        lastModifiedMap.clear();
    }
}