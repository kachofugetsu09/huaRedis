package site.hnfy258.storage;

import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.datatype.RedisData;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class OptimizedRedisHashSlots {
    private static final int SLOTS_SIZE = 16384; // Redis标准槽数量
    private final ConcurrentHashMap<Integer, ConcurrentHashMap<BytesWrapper, RedisData>> slots;
    private final ReadWriteLock[] locks;

    private static final int[] CRC16_TABLE = new int[256];

    static {
        for (int i = 0; i < 256; i++) {
            int crc = i;
            for (int j = 0; j < 8; j++) {
                if ((crc & 0x8000) != 0) {
                    crc = (crc << 1) ^ 0x1021;
                } else {
                    crc <<= 1;
                }
            }
            CRC16_TABLE[i] = crc & 0xFFFF;
        }
    }

    public OptimizedRedisHashSlots() {
        slots = new ConcurrentHashMap<>(SLOTS_SIZE);
        locks = new ReadWriteLock[SLOTS_SIZE];
        for (int i = 0; i < SLOTS_SIZE; i++) {
            slots.put(i, new ConcurrentHashMap<>());
            locks[i] = new ReentrantReadWriteLock();
        }
    }

    private int getSlot(BytesWrapper key) {
        return crc16(key) & (SLOTS_SIZE - 1);
    }

    private int crc16(BytesWrapper key) {
        int crc = 0;
        for (byte b : key.getBytes()) {
            crc = ((crc << 8) ^ CRC16_TABLE[((crc >>> 8) ^ (b & 0xFF)) & 0xFF]) & 0xFFFF;
        }
        return crc;
    }

    public void put(BytesWrapper key, RedisData value) {
        int slot = getSlot(key);
        locks[slot].writeLock().lock();
        try {
            slots.get(slot).put(key, value);
        } finally {
            locks[slot].writeLock().unlock();
        }
    }

    public RedisData get(BytesWrapper key) {
        int slot = getSlot(key);
        locks[slot].readLock().lock();
        try {
            return slots.get(slot).get(key);
        } finally {
            locks[slot].readLock().unlock();
        }
    }

    public boolean containsKey(BytesWrapper key) {
        int slot = getSlot(key);
        locks[slot].readLock().lock();
        try {
            return slots.get(slot).containsKey(key);
        } finally {
            locks[slot].readLock().unlock();
        }
    }

    public RedisData remove(BytesWrapper key) {
        int slot = getSlot(key);
        locks[slot].writeLock().lock();
        try {
            return slots.get(slot).remove(key);
        } finally {
            locks[slot].writeLock().unlock();
        }
    }

    public long removeAll(List<BytesWrapper> keys) {
        long count = 0;
        for (BytesWrapper key : keys) {
            if (remove(key) != null) {
                count++;
            }
        }
        return count;
    }

    public Set<BytesWrapper> keySet() {
        Set<BytesWrapper> keys = ConcurrentHashMap.newKeySet();
        for (int i = 0; i < SLOTS_SIZE; i++) {
            locks[i].readLock().lock();
            try {
                keys.addAll(slots.get(i).keySet());
            } finally {
                locks[i].readLock().unlock();
            }
        }
        return keys;
    }

    public void clear() {
        for (int i = 0; i < SLOTS_SIZE; i++) {
            locks[i].writeLock().lock();
            try {
                slots.get(i).clear();
            } finally {
                locks[i].writeLock().unlock();
            }
        }
    }
}