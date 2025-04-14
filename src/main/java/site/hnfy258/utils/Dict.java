package site.hnfy258.utils;

import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.datatype.RedisData;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * Redis风格的Dict实现，支持渐进式rehash
 */
public class Dict<K, V> {
    private static final int INITIAL_SIZE = 4;
    private static final int FORCE_REHASH_RATIO = 2; // 负载因子超过2时强制rehash
    private static final int REHASH_MIN_SLAVES = 5; // 每次增量rehash至少迁移5个entry

    private DictHashTable<K, V> ht0;
    private DictHashTable<K, V> ht1;
    private int rehashIndex; // 当前rehash到的索引位置，-1表示没有在进行rehash
    private int iterators; // 当前活跃的迭代器数量
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * 哈希表节点结构
     */
    static class DictEntry<K, V> {
        K key;
        V value;
        DictEntry<K, V> next; // 链式解决冲突

        DictEntry(K key, V value) {
            this.key = key;
            this.value = value;
            this.next = null;
        }
    }

    /**
     * 哈希表结构
     */
    static class DictHashTable<K, V> {
        DictEntry<K, V>[] table;
        int size; // 哈希表大小
        int sizemask; // size - 1, 用于计算哈希索引
        int used; // 已使用的键值对数量

        @SuppressWarnings("unchecked")
        DictHashTable(int size) {
            this.size = size;
            this.sizemask = size - 1;
            this.used = 0;
            this.table = (DictEntry<K, V>[]) new DictEntry[size];
        }
    }

    public Dict() {
        ht0 = new DictHashTable<>(INITIAL_SIZE);
        ht1 = null;
        rehashIndex = -1;
        iterators = 0;
    }

    /**
     * 获取键的哈希值
     */
    private int hash(Object key) {
        if (key == null) return 0;
        int h = key.hashCode();
        return h ^ (h >>> 16); // 同Java HashMap的哈希计算方式
    }

    /**
     * 获取键在哈希表中的索引
     */
    private int keyIndex(Object key, int capacity) {
        return hash(key) & (capacity - 1);
    }

    /**
     * 查找键的位置，返回对应的DictEntry
     */
    private DictEntry<K, V> find(K key) {
        if (key == null) return null;

        // 如果正在rehash，执行一步渐进式rehash
        if (rehashIndex != -1) rehashStep();

        // 先从ht0查找
        int idx = keyIndex(key, ht0.size);
        DictEntry<K, V> entry = ht0.table[idx];
        while (entry != null) {
            if (key.equals(entry.key)) return entry;
            entry = entry.next;
        }

        // 如果在进行rehash，再从ht1查找
        if (rehashIndex != -1 && ht1 != null) {
            idx = keyIndex(key, ht1.size);
            entry = ht1.table[idx];
            while (entry != null) {
                if (key.equals(entry.key)) return entry;
                entry = entry.next;
            }
        }

        return null;
    }

    /**
     * 放入键值对
     */
    public V put(K key, V value) {
        if (key == null) throw new IllegalArgumentException("Key cannot be null");

        lock.lock();
        try {
            // 检查是否需要扩容
            if (rehashIndex == -1) {
                // 计算负载因子
                double loadFactor = (double) ht0.used / ht0.size;
                if (loadFactor >= FORCE_REHASH_RATIO) {
                    // 开始rehash，新表大小为已使用键数的两倍
                    startRehash(ht0.used * 2);
                }
            }

            // 如果正在rehash，推进一步
            if (rehashIndex != -1) rehashStep();

            V oldValue = null;
            DictEntry<K, V> entry = find(key);

            // 如果找到键，更新值
            if (entry != null) {
                oldValue = entry.value;
                entry.value = value;
                return oldValue;
            }

            // 键不存在，添加新值
            int idx;
            // 如果正在rehash，添加到ht1
            if (rehashIndex != -1) {
                idx = keyIndex(key, ht1.size);
                DictEntry<K, V> newEntry = new DictEntry<>(key, value);
                newEntry.next = ht1.table[idx];
                ht1.table[idx] = newEntry;
                ht1.used++;
            } else {
                // 否则添加到ht0
                idx = keyIndex(key, ht0.size);
                DictEntry<K, V> newEntry = new DictEntry<>(key, value);
                newEntry.next = ht0.table[idx];
                ht0.table[idx] = newEntry;
                ht0.used++;
            }

            return null;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Te 获取键对应的值
     */
    public V get(K key) {
        DictEntry<K, V> entry = find(key);
        return entry != null ? entry.value : null;
    }

    /**
     * 检查键是否存在
     */
    public boolean containsKey(K key) {
        return find(key) != null;
    }

    /**
     * 删除键，返回删除的值
     */
    public V remove(K key) {
        if (key == null) return null;

        lock.lock();
        try {
            // 如果正在rehash，推进一步
            if (rehashIndex != -1) rehashStep();

            // 从ht0中查找并删除
            int idx = keyIndex(key, ht0.size);
            DictEntry<K, V> entry = ht0.table[idx];
            DictEntry<K, V> prev = null;

            while (entry != null) {
                if (key.equals(entry.key)) {
                    if (prev == null) {
                        ht0.table[idx] = entry.next;
                    } else {
                        prev.next = entry.next;
                    }
                    ht0.used--;
                    return entry.value;
                }
                prev = entry;
                entry = entry.next;
            }

            // 如果在进行rehash，再从ht1中查找并删除
            if (rehashIndex != -1 && ht1 != null) {
                idx = keyIndex(key, ht1.size);
                entry = ht1.table[idx];
                prev = null;

                while (entry != null) {
                    if (key.equals(entry.key)) {
                        if (prev == null) {
                            ht1.table[idx] = entry.next;
                        } else {
                            prev.next = entry.next;
                        }
                        ht1.used--;
                        return entry.value;
                    }
                    prev = entry;
                    entry = entry.next;
                }
            }

            return null;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 获取字典中的键集合
     */
    public Set<K> keySet() {
        Set<K> keys = new HashSet<>();

        // 如果正在rehash，推进一步
        if (rehashIndex != -1) rehashStep();

        // 从ht0收集键
        for (int i = 0; i < ht0.size; i++) {
            DictEntry<K, V> entry = ht0.table[i];
            while (entry != null) {
                keys.add(entry.key);
                entry = entry.next;
            }
        }

        // 如果在rehash，从ht1也收集键
        if (rehashIndex != -1 && ht1 != null) {
            for (int i = 0; i < ht1.size; i++) {
                DictEntry<K, V> entry = ht1.table[i];
                while (entry != null) {
                    keys.add(entry.key);
                    entry = entry.next;
                }
            }
        }

        return keys;
    }

    /**
     * 获取所有键值对
     */
    public Map<K, V> getAll() {
        Map<K, V> map = new HashMap<>();

        // 如果正在rehash，推进一步
        if (rehashIndex != -1) rehashStep();

        // 从ht0收集键值对
        for (int i = 0; i < ht0.size; i++) {
            DictEntry<K, V> entry = ht0.table[i];
            while (entry != null) {
                map.put(entry.key, entry.value);
                entry = entry.next;
            }
        }

        // 如果在rehash，从ht1也收集键值对
        if (rehashIndex != -1 && ht1 != null) {
            for (int i = 0; i < ht1.size; i++) {
                DictEntry<K, V> entry = ht1.table[i];
                while (entry != null) {
                    map.put(entry.key, entry.value);
                    entry = entry.next;
                }
            }
        }

        return map;
    }

    /**
     * 清空字典
     */
    public void clear() {
        lock.lock();
        try {
            ht0 = new DictHashTable<>(INITIAL_SIZE);
            ht1 = null;
            rehashIndex = -1;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 获取字典大小
     */
    public int size() {
        return ht0.used + (ht1 != null ? ht1.used : 0);
    }

    /**
     * 开始rehash过程
     */
    private void startRehash(int newSize) {
        // 确保新大小是2的幂
        int realSize = 1;
        while (realSize < newSize) {
            realSize *= 2;
        }

        ht1 = new DictHashTable<>(realSize);
        rehashIndex = 0;
    }

    /**
     * 执行一步渐进式rehash
     */
    private void rehashStep() {
        if (rehashIndex == -1 || ht1 == null) return;

        lock.lock();
        try {
            // 每次最多迁移REHASH_MIN_SLAVES个桶
            int emptyVisited = 0;
            int processed = 0;

            while (emptyVisited < 10 && processed < REHASH_MIN_SLAVES && rehashIndex < ht0.size) {
                // 跳过空桶
                if (ht0.table[rehashIndex] == null) {
                    rehashIndex++;
                    emptyVisited++;
                    continue;
                }

                // 迁移当前桶的所有元素到ht1
                DictEntry<K, V> entry = ht0.table[rehashIndex];
                while (entry != null) {
                    DictEntry<K, V> next = entry.next;

                    // 重新计算新哈希表中的位置
                    int idx = keyIndex(entry.key, ht1.size);
                    entry.next = ht1.table[idx];
                    ht1.table[idx] = entry;
                    ht0.used--;
                    ht1.used++;

                    entry = next;
                }

                // 清空原桶
                ht0.table[rehashIndex] = null;
                rehashIndex++;
                processed++;
            }

            // 检查是否完成所有rehash
            if (rehashIndex >= ht0.size) {
                // rehash完成，ht1变成ht0
                ht0 = ht1;
                ht1 = null;
                rehashIndex = -1;
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 批量删除多个键
     */
    public long removeAll(List<K> keys) {
        if (keys == null || keys.isEmpty()) return 0;

        long count = 0;
        for (K key : keys) {
            if (remove(key) != null) {
                count++;
            }
        }
        return count;
    }
}




