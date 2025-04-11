package site.hnfy258.rdb.policy;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class SavePolicy {
    public static class SavePoint {
        final int seconds;
        final int changes;

        public SavePoint(int seconds, int changes) {
            this.seconds = seconds;
            this.changes = changes;
        }
    }

    private final List<SavePoint> savePoints;
    private final Map<SavePoint, Long> lastSaveTimeMap = new ConcurrentHashMap<>();
    private final Map<SavePoint, AtomicInteger> changeCountMap = new ConcurrentHashMap<>();
    private long lastFullSaveTime = 0;

    public SavePolicy() {
        this.savePoints = new ArrayList<>();
        // 默认配置
        savePoints.add(new SavePoint(900, 1));
        savePoints.add(new SavePoint(300, 10));
        savePoints.add(new SavePoint(60, 10000));
    }

    public SavePolicy(List<SavePoint> savePoints) {
        this.savePoints = new ArrayList<>(savePoints);
    }

    public void recordChange() {
        for (SavePoint sp : savePoints) {
            AtomicInteger counter = changeCountMap.computeIfAbsent(sp, k -> new AtomicInteger(0));
            counter.incrementAndGet();
        }
    }

    public boolean shouldFullSave(long currentTimeSeconds) {
        boolean hourlySave = (currentTimeSeconds - lastFullSaveTime) >= 3600;
        if (hourlySave) {
            lastFullSaveTime = currentTimeSeconds;
            return true;
        }

        for (SavePoint sp : savePoints) {
            long lastSaveTime = lastSaveTimeMap.getOrDefault(sp, 0L);
            AtomicInteger changes = changeCountMap.computeIfAbsent(sp, k -> new AtomicInteger(0));

            if (currentTimeSeconds - lastSaveTime >= sp.seconds && changes.get() >= sp.changes) {
                changeCountMap.get(sp).set(0);
                lastSaveTimeMap.put(sp, currentTimeSeconds);
                return true;
            }
        }
        return false;
    }

    public void reset() {
        lastSaveTimeMap.clear();
        changeCountMap.clear();
        lastFullSaveTime = 0;
    }
}