package site.hnfy258.rdb.core;

import site.hnfy258.RedisCore;
import site.hnfy258.datatype.*;
import site.hnfy258.rdb.constants.RDBConstants;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class RDBLoader implements Loader {
    private final RedisCore redisCore;
    private static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(RDBLoader.class);

    public RDBLoader(RedisCore redisCore) {
        this.redisCore = redisCore;
    }

    public void loadRDB(File file) throws IOException {
        if (!file.exists()) {
            logger.info("RDB文件不存在: " + file.getName());
            return;
        }

        try (DataInputStream dis = new DataInputStream(new FileInputStream(file))) {
            if (!RDBUtil.validateRDBHeader(dis)) {
                logger.warn("无效的RDB文件格式: " + file.getName());
                return;
            }
            loadData(dis);
        } catch (EOFException e) {
            logger.error("RDB文件读取时遇到意外的文件结束", e);
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
                    currentDb = (int) RDBUtil.readLength(dis);
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
        BytesWrapper key = new BytesWrapper(RDBUtil.readString(dis));
        BytesWrapper value = new BytesWrapper(RDBUtil.readString(dis));
        redisCore.setDB(currentDb, key, new RedisString(value));
        logger.debug("加载键值对到数据库" + currentDb + ": " + key);
    }

    private void loadList(DataInputStream dis, int currentDb) throws IOException {
        BytesWrapper key = new BytesWrapper(RDBUtil.readString(dis));
        long size = RDBUtil.readLength(dis);
        RedisList list = new RedisList();
        for (int i = 0; i < size; i++) {
            list.rpush(new BytesWrapper(RDBUtil.readString(dis)));
        }
        redisCore.setDB(currentDb, key, list);
        logger.debug("加载列表到数据库" + currentDb + ": " + key + " (元素数: " + size + ")");
    }

    private void loadSet(DataInputStream dis, int currentDb) throws IOException {
        BytesWrapper key = new BytesWrapper(RDBUtil.readString(dis));
        long size = RDBUtil.readLength(dis);
        RedisSet set = new RedisSet();
        List<BytesWrapper> elements = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            elements.add(new BytesWrapper(RDBUtil.readString(dis)));
        }
        set.sadd(elements);
        redisCore.setDB(currentDb, key, set);
        logger.debug("加载集合到数据库" + currentDb + ": " + key + " (元素数: " + size + ")");
    }

    private void loadHash(DataInputStream dis, int currentDb) throws IOException {
        BytesWrapper key = new BytesWrapper(RDBUtil.readString(dis));
        long size = RDBUtil.readLength(dis);
        RedisHash hash = new RedisHash();
        for (int i = 0; i < size; i++) {
            BytesWrapper field = new BytesWrapper(RDBUtil.readString(dis));
            BytesWrapper value = new BytesWrapper(RDBUtil.readString(dis));
            hash.put(field, value);
        }
        redisCore.setDB(currentDb, key, hash);
        logger.debug("加载哈希表到数据库" + currentDb + ": " + key + " (字段数: " + size + ")");
    }

    private void loadZset(DataInputStream dis, int currentDb) throws IOException {
        BytesWrapper key = new BytesWrapper(RDBUtil.readString(dis));
        long size = RDBUtil.readLength(dis);
        RedisZset zset = new RedisZset();
        for (int i = 0; i < size; i++) {
            String member = new String(RDBUtil.readString(dis), StandardCharsets.UTF_8);
            double score = dis.readDouble();
            zset.add(score, member);
        }
        redisCore.setDB(currentDb, key, zset);
        logger.debug("加载有序集合到数据库" + currentDb + ": " + key + " (元素数: " + size + ")");
    }

    public void clearAllDatabases() {
        for (int i = 0; i < redisCore.getDbNum(); i++) {
            redisCore.getDBData(i).clear();
        }
    }
}