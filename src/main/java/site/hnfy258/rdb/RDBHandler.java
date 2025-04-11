package site.hnfy258.rdb;

import site.hnfy258.RedisCore;
import site.hnfy258.datatype.*;
import site.hnfy258.utiils.SkipList;

import java.io.*;
import java.nio.charset.StandardCharsets;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

public class RDBHandler {
    private static final Logger logger = Logger.getLogger(RDBHandler.class);
    private final RedisCore redisCore;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService saveExecutor;
    private volatile boolean isSaving = false;
    private final List<SavePoint> savePoints = new ArrayList<>();

    private static final String FULL_RDB_FILE_NAME = RDBConstants.RDB_FILE_NAME;
    private static final String INCREMENTAL_RDB_FILE_NAME = RDBConstants.RDB_FILE_NAME + ".inc";
    private long lastFullSaveTime = 0;
    private final Map<Integer, Map<BytesWrapper, Long>> lastModifiedMap = new ConcurrentHashMap<>();

    public boolean isSaving() {
        return isSaving;
    }


    // 定义RDB保存条件
    public static class SavePoint {
        final int seconds;
        final int changes;

        public SavePoint(int seconds, int changes) {
            this.seconds = seconds;
            this.changes = changes;
        }
    }

    public RDBHandler(RedisCore redisCore) {
        this.redisCore = redisCore;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "rdb-scheduler");
            t.setDaemon(true);
            return t;
        });
        this.saveExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "rdb-saver");
            t.setDaemon(true);
            return t;
        });

        // 添加默认的保存点配置，类似Redis的默认配置
        savePoints.add(new SavePoint(900, 1));   // 900秒内有1次修改
        savePoints.add(new SavePoint(300, 10));  // 300秒内有10次修改
        savePoints.add(new SavePoint(60, 10000)); // 60秒内有10000次修改
    }

    public void initialize() {
        try {
            load();
            startAutoSave();
        } catch (IOException e) {
            logger.error("初始化RDB处理器失败", e);
        }
    }

    private void startAutoSave() {
        // 每秒检查一次是否需要触发保存
        scheduler.scheduleAtFixedRate(() -> {
            try {
                checkSaveConditions();
            } catch (Exception e) {
                logger.error("自动保存检查失败", e);
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    private final Map<SavePoint, Long> lastSaveTimeMap = new ConcurrentHashMap<>();
    private final Map<SavePoint, AtomicInteger> changeCountMap = new ConcurrentHashMap<>();

    private void checkSaveConditions() {
        if (isSaving) return;

        long now = System.currentTimeMillis() / 1000;
        boolean needFullSave = false;
        SavePoint triggeredPoint = null;

        // 检查所有保存点条件
        for (SavePoint sp : savePoints) {
            long lastSaveTime = lastSaveTimeMap.getOrDefault(sp, 0L);
            AtomicInteger changes = changeCountMap.computeIfAbsent(sp, k -> new AtomicInteger(0));

            if (now - lastSaveTime >= sp.seconds && changes.get() >= sp.changes) {
                needFullSave = true;
                triggeredPoint = sp;
                break;
            }
        }

        // 每小时强制全量保存
        boolean hourlySave = (now - lastFullSaveTime / 1000) >= 3600;

        if (needFullSave || hourlySave) {
            if (triggeredPoint != null) {
                // 重置触发点的计数器和时间戳
                changeCountMap.get(triggeredPoint).set(0);
                lastSaveTimeMap.put(triggeredPoint, now);
            }
            if (hourlySave) {
                lastFullSaveTime = System.currentTimeMillis();
            }
            bgsave(true);
        } if (!lastModifiedMap.isEmpty() && lastModifiedMap.values().stream()
                .mapToInt(Map::size).sum() > 100) {  // 至少100个键被修改
            bgsave(false);
        }
    }

    // 记录数据变更，供自动保存使用
    public void notifyDataChanged(int dbIndex, BytesWrapper key) {
        for (SavePoint sp : savePoints) {
            AtomicInteger counter = changeCountMap.computeIfAbsent(sp, k -> new AtomicInteger(0));
            counter.incrementAndGet();
        }
        lastModifiedMap.computeIfAbsent(dbIndex, k -> new ConcurrentHashMap<>())
                .put(key, System.currentTimeMillis());
    }

    private void doIncrementalSave() throws IOException {
        File tempFile = new File(INCREMENTAL_RDB_FILE_NAME + ".tmp");

        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(tempFile))) {
            writeRDBHeader(dos);

            for (Map.Entry<Integer, Map<BytesWrapper, Long>> dbEntry : lastModifiedMap.entrySet()) {
                int dbIndex = dbEntry.getKey();
                Map<BytesWrapper, Long> modifiedKeys = dbEntry.getValue();

                if (!modifiedKeys.isEmpty()) {
                    writeSelectDB(dos, dbIndex);
                    Map<BytesWrapper, RedisData> dbData = redisCore.getDBData(dbIndex);

                    for (Map.Entry<BytesWrapper, Long> entry : modifiedKeys.entrySet()) {
                        BytesWrapper key = entry.getKey();
                        RedisData value = dbData.get(key);
                        if (value != null) {
                            saveEntry(dos, key, value);
                        }
                    }
                }
            }

            writeRDBFooter(dos);
        }

        // 原子性地替换文件
        File incRdbFile = new File(INCREMENTAL_RDB_FILE_NAME);
        if (!tempFile.renameTo(incRdbFile)) {
            // 如果重命名失败，尝试复制内容并删除临时文件
            try (InputStream in = new FileInputStream(tempFile);
                 OutputStream out = new FileOutputStream(incRdbFile)) {
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = in.read(buffer)) != -1) {
                    out.write(buffer, 0, bytesRead);
                }
            }
            tempFile.delete();
        }

        // 清除已保存的增量数据
        lastModifiedMap.clear();
    }

    public CompletableFuture<Boolean> save() {
        CompletableFuture<Boolean> result = new CompletableFuture<>();

        if (isSaving) {
            logger.warn("已有RDB保存任务在进行中，转为异步等待");
            // 异步检查是否完成
            new Thread(() -> {
                try {
                    int maxWaitSeconds = 30;  // 最大等待30秒
                    while (isSaving && maxWaitSeconds-- > 0) {
                        Thread.sleep(1000);
                    }
                    if (!isSaving) {
                        doSave();
                        result.complete(true);
                    } else {
                        result.completeExceptionally(new TimeoutException("等待RDB保存超时"));
                    }
                } catch (Exception e) {
                    result.completeExceptionally(e);
                }
            }, "save-async-waiter").start();
            return result;
        }

        // 无冲突时直接执行
        try {
            logger.info("开始同步保存RDB文件");
            doSave();
            result.complete(true);
        } catch (IOException e) {
            result.completeExceptionally(e);
        }
        return result;
    }

    public boolean bgsave(boolean fullSave) {
        if (isSaving) {
            logger.warn("已有RDB保存任务在进行中，忽略此次请求");
            return false;
        }

        isSaving = true;
        saveExecutor.submit(() -> {
            try {
                if (fullSave) {
                    Map<Integer, Map<BytesWrapper, RedisData>> snapshot = new HashMap<>();
                    for (Integer dbIndex : lastModifiedMap.keySet()) {
                        Map<BytesWrapper, RedisData> dbData = redisCore.getDBData(dbIndex);
                        snapshot.put(dbIndex, filterUnmodifiedData(dbData, lastModifiedMap.get(dbIndex)));
                    }
                    doSaveWithSnapshot(snapshot);
                } else {
                    logger.info("开始后台增量保存RDB文件");
                    doIncrementalSave();
                    logger.info("RDB文件增量后台保存完成");
                }
            } catch (Exception e) {
                logger.error("后台保存RDB文件失败", e);
            } finally {
                isSaving = false;
            }
        });

        return true;
    }

    private Map<BytesWrapper, RedisData> filterUnmodifiedData(
            Map<BytesWrapper, RedisData> dbData,
            Map<BytesWrapper, Long> modifiedKeys) {

        return dbData.entrySet().stream()
                .filter(e -> modifiedKeys.containsKey(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
    // 创建数据快照
    private Map<Integer, Map<BytesWrapper, RedisData>> createSnapshot() {
        Map<Integer, Map<BytesWrapper, RedisData>> snapshot = new HashMap<>();
        for (int dbIndex = 0; dbIndex < redisCore.getDbNum(); dbIndex++) {
            Map<BytesWrapper, RedisData> dbData = redisCore.getDBData(dbIndex);
            if (!dbData.isEmpty()) {
                // 分批处理：每批最多 1000 个键值对
                Map<BytesWrapper, RedisData> dbCopy = new HashMap<>();
                int batchSize = 0;
                for (Map.Entry<BytesWrapper, RedisData> entry : dbData.entrySet()) {
                    RedisData value = entry.getValue();
                    if (value.isImmutable()) {
                        dbCopy.put(entry.getKey(), value); // 不可变对象直接引用
                    } else {
                        dbCopy.put(entry.getKey(), value.deepCopy()); // 可变对象深拷贝
                    }

                    // 每满 1000 个键值对，暂停一下（避免长时间阻塞）
                    if (++batchSize % 1000 == 0) {
                        Thread.yield(); // 让出CPU，减少对主线程的影响
                    }
                }
                snapshot.put(dbIndex, dbCopy);
            }
        }
        return snapshot;
    }

    // 使用快照进行保存
    private void doSaveWithSnapshot(Map<Integer, Map<BytesWrapper, RedisData>> snapshot) throws IOException {
        File tempFile = new File(RDBConstants.RDB_FILE_NAME + ".tmp");

        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(tempFile))) {
            writeRDBHeader(dos);

            // 保存所有数据库
            for (Map.Entry<Integer, Map<BytesWrapper, RedisData>> entry : snapshot.entrySet()) {
                int dbIndex = entry.getKey();
                Map<BytesWrapper, RedisData> dbData = entry.getValue();

                if (!dbData.isEmpty()) {
                    writeSelectDB(dos, dbIndex);
                    saveDatabase(dos, dbData);
                }
            }

            writeRDBFooter(dos);
        }

        // 原子性地替换文件
        File rdbFile = new File(RDBConstants.RDB_FILE_NAME);
        if (!tempFile.renameTo(rdbFile)) {
            // 如果重命名失败，尝试复制内容并删除临时文件
            try (InputStream in = new FileInputStream(tempFile);
                 OutputStream out = new FileOutputStream(rdbFile)) {
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = in.read(buffer)) != -1) {
                    out.write(buffer, 0, bytesRead);
                }
            }
            tempFile.delete();
        }
    }

    // 直接保存当前数据，用于同步保存
    private void doSave() throws IOException {
        File tempFile = new File(RDBConstants.RDB_FILE_NAME + ".tmp");

        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(tempFile))) {
            writeRDBHeader(dos);
            saveAllDatabases(dos);
            writeRDBFooter(dos);
        }

        // 原子性地替换文件
        File rdbFile = new File(RDBConstants.RDB_FILE_NAME);
        if (!tempFile.renameTo(rdbFile)) {
            // 如果重命名失败，尝试复制内容并删除临时文件
            try (InputStream in = new FileInputStream(tempFile);
                 OutputStream out = new FileOutputStream(rdbFile)) {
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = in.read(buffer)) != -1) {
                    out.write(buffer, 0, bytesRead);
                }
            }
            tempFile.delete();
        }
    }

    private void writeRDBHeader(DataOutputStream dos) throws IOException {
        dos.writeBytes("REDIS0001");
    }

    private void saveAllDatabases(DataOutputStream dos) throws IOException {
        for (int dbIndex = 0; dbIndex < redisCore.getDbNum(); dbIndex++) {
            Map<BytesWrapper, RedisData> dbData = redisCore.getDBData(dbIndex);
            if (!dbData.isEmpty()) {
                writeSelectDB(dos, dbIndex);
                saveDatabase(dos, dbData);
            }
        }
    }

    private void writeSelectDB(DataOutputStream dos, int dbIndex) throws IOException {
        dos.writeByte(RDBConstants.RDB_OPCODE_SELECTDB);
        writeLength(dos, dbIndex);
    }

    private void saveDatabase(DataOutputStream dos, Map<BytesWrapper, RedisData> dbData) throws IOException {
        for (Map.Entry<BytesWrapper, RedisData> entry : dbData.entrySet()) {
            BytesWrapper key = entry.getKey();
            RedisData value = entry.getValue();
            saveEntry(dos, key, value);
        }
    }

    private void saveEntry(DataOutputStream dos, BytesWrapper key, RedisData value) throws IOException {
        switch (value.getClass().getSimpleName()) {
            case "RedisString":
                saveString(dos, key, (RedisString) value);
                break;
            case "RedisList":
                saveList(dos, key, (RedisList) value);
                break;
            case "RedisSet":
                saveSet(dos, key, (RedisSet) value);
                break;
            case "RedisHash":
                saveHash(dos, key, (RedisHash) value);
                break;
            case "RedisZset":
                saveZset(dos, key, (RedisZset) value);
                break;
            default:
                logger.warn("未知的数据类型: " + value.getClass().getSimpleName());
        }
    }

    private void saveString(DataOutputStream dos, BytesWrapper key, RedisString value) throws IOException {
        dos.writeByte(RDBConstants.STRING_TYPE);
        writeString(dos, key.getBytes());
        writeString(dos, value.getValue().getBytes());
    }

    private void saveList(DataOutputStream dos, BytesWrapper key, RedisList value) throws IOException {
        dos.writeByte(RDBConstants.LIST_TYPE);
        writeString(dos, key.getBytes());
        writeLength(dos, value.size());
        for (BytesWrapper element : value.getAllElements()) {
            writeString(dos, element.getBytes());
        }
    }

    private void saveSet(DataOutputStream dos, BytesWrapper key, RedisSet value) throws IOException {
        dos.writeByte(RDBConstants.SET_TYPE);
        writeString(dos, key.getBytes());
        writeLength(dos, value.size());
        for (BytesWrapper element : value.keys()) {
            writeString(dos, element.getBytes());
        }
    }

    private void saveHash(DataOutputStream dos, BytesWrapper key, RedisHash value) throws IOException {
        dos.writeByte(RDBConstants.HASH_TYPE);
        writeString(dos, key.getBytes());
        Map<BytesWrapper, BytesWrapper> hashMap = value.getMap();
        writeLength(dos, hashMap.size());
        for (Map.Entry<BytesWrapper, BytesWrapper> e : hashMap.entrySet()) {
            writeString(dos, e.getKey().getBytes());
            writeString(dos, e.getValue().getBytes());
        }
    }

    private void saveZset(DataOutputStream dos, BytesWrapper key, RedisZset value) throws IOException {
        dos.writeByte(RDBConstants.ZSET_TYPE);
        writeString(dos, key.getBytes());
        int zsetSize = value.size();
        writeLength(dos, zsetSize);
        List<SkipList.Node> allNodes = value.getRange(0, zsetSize - 1);
        for (SkipList.Node node : allNodes) {
            writeString(dos, node.member.getBytes());
            dos.writeDouble(node.score);
        }
    }

    private void writeRDBFooter(DataOutputStream dos) throws IOException {
        dos.writeByte(RDBConstants.RDB_OPCODE_EOF);
        dos.writeLong(0); // CRC64校验和，目前未处理
    }

    public void load() throws IOException {
        File fullFile = new File(FULL_RDB_FILE_NAME);
        File incFile = new File(INCREMENTAL_RDB_FILE_NAME);

        if (!fullFile.exists() && !incFile.exists()) {
            logger.info("RDB文件不存在，跳过加载过程");
            return;
        }

        logger.info("开始加载RDB文件");
        clearAllDatabases();

        if (fullFile.exists()) {
            loadFile(fullFile);
        }

        if (incFile.exists()) {
            loadFile(incFile);
        }

        logger.info("RDB文件加载成功");
    }

    private void loadFile(File file) throws IOException {
        try (DataInputStream dis = new DataInputStream(new FileInputStream(file))) {
            if (!validateRDBHeader(dis)) {
                return;
            }
            loadData(dis);
        } catch (EOFException e) {
            logger.error("RDB文件读取时遇到意外的文件结束", e);
        } catch (IOException e) {
            logger.error("加载RDB文件时发生错误", e);
            throw e;
        }
    }
    private boolean validateRDBHeader(DataInputStream dis) throws IOException {
        byte[] header = new byte[9];
        int bytesRead = dis.read(header);
        if (bytesRead != 9 || !"REDIS0001".equals(new String(header, StandardCharsets.UTF_8))) {
            logger.warn("无效的RDB文件格式");
            return false;
        }
        return true;
    }

    private void clearAllDatabases() {
        for (int i = 0; i < redisCore.getDbNum(); i++) {
            redisCore.getDBData(i).clear();
        }
    }

    private void loadData(DataInputStream dis) throws IOException {
        int currentDb = 0;
        while (true) {
            int type = dis.read();
            if (type == -1 || (byte)type == RDBConstants.RDB_OPCODE_EOF) {
                break;
            }

            switch ((byte)type) {
                case RDBConstants.RDB_OPCODE_SELECTDB:
                    currentDb = (int) readLength(dis);
                    logger.debug("切换到数据库: " + currentDb);
                    break;
                case RDBConstants.STRING_TYPE:
                    loadString(dis, currentDb);
                    break;
                case RDBConstants.LIST_TYPE:
                    loadList(dis, currentDb);
                    break;
                case RDBConstants.SET_TYPE:
                    loadSet(dis, currentDb);
                    break;
                case RDBConstants.HASH_TYPE:
                    loadHash(dis, currentDb);
                    break;
                case RDBConstants.ZSET_TYPE:
                    loadZset(dis, currentDb);
                    break;
                case RDBConstants.RDB_OPCODE_EXPIRETIME:
                case RDBConstants.RDB_OPCODE_EXPIRETIME_MS:
                    logger.warn("暂不支持过期时间，跳过");
                    dis.readInt(); // 跳过过期时间
                    break;
                default:
                    logger.warn("未知的数据类型: " + type);
                    return;
            }
        }
    }

    private void loadString(DataInputStream dis, int currentDb) throws IOException {
        BytesWrapper key = new BytesWrapper(readString(dis));
        BytesWrapper value = new BytesWrapper(readString(dis));
        redisCore.setDB(currentDb, key, new RedisString(value));
        logger.debug("加载键值对到数据库" + currentDb + ": " + key);
    }

    private void loadList(DataInputStream dis, int currentDb) throws IOException {
        BytesWrapper key = new BytesWrapper(readString(dis));
        long size = readLength(dis);
        RedisList list = new RedisList();
        for (int i = 0; i < size; i++) {
            list.rpush(new BytesWrapper(readString(dis)));
        }
        redisCore.setDB(currentDb, key, list);
        logger.debug("加载列表到数据库" + currentDb + ": " + key + " (元素数: " + size + ")");
    }

    private void loadSet(DataInputStream dis, int currentDb) throws IOException {
        BytesWrapper key = new BytesWrapper(readString(dis));
        long size = readLength(dis);
        RedisSet set = new RedisSet();
        List<BytesWrapper> elements = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            elements.add(new BytesWrapper(readString(dis)));
        }
        set.sadd(elements);
        redisCore.setDB(currentDb, key, set);
        logger.debug("加载集合到数据库" + currentDb + ": " + key + " (元素数: " + size + ")");
    }

    private void loadHash(DataInputStream dis, int currentDb) throws IOException {
        BytesWrapper key = new BytesWrapper(readString(dis));
        long size = readLength(dis);
        RedisHash hash = new RedisHash();
        for (int i = 0; i < size; i++) {
            BytesWrapper field = new BytesWrapper(readString(dis));
            BytesWrapper value = new BytesWrapper(readString(dis));
            hash.put(field, value);
        }
        redisCore.setDB(currentDb, key, hash);
        logger.debug("加载哈希表到数据库" + currentDb + ": " + key + " (字段数: " + size + ")");
    }

    private void loadZset(DataInputStream dis, int currentDb) throws IOException {
        BytesWrapper key = new BytesWrapper(readString(dis));
        long size = readLength(dis);
        RedisZset zset = new RedisZset();
        for (int i = 0; i < size; i++) {
            String member = new String(readString(dis), StandardCharsets.UTF_8);
            double score = dis.readDouble();
            zset.add(score, member);
        }
        redisCore.setDB(currentDb, key, zset);
        logger.debug("加载有序集合到数据库" + currentDb + ": " + key + " (元素数: " + size + ")");
    }

    private void writeString(DataOutputStream dos, byte[] str) throws IOException {
        writeLength(dos, str.length);
        dos.write(str);
    }

    private byte[] readString(DataInputStream dis) throws IOException {
        long length = readLength(dis);
        if (length < 0 || length > Integer.MAX_VALUE) {
            throw new IOException("Invalid string length: " + length);
        }
        byte[] str = new byte[(int) length];
        dis.readFully(str);
        return str;
    }

    private void writeLength(DataOutputStream dos, long length) throws IOException {
        dos.writeInt((int) length);
    }

    private long readLength(DataInputStream dis) throws IOException {
        return dis.readInt() & 0xFFFFFFFFL;
    }

    public void shutdown() {
        scheduler.shutdown();
        saveExecutor.shutdown();
        try {
            // 等待任务完成
            if (!saveExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                saveExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            saveExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}