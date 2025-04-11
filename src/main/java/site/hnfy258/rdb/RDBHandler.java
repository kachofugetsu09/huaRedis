package site.hnfy258.rdb;

import site.hnfy258.RedisCore;
import site.hnfy258.datatype.*;
import site.hnfy258.utiils.SkipList;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

public class RDBHandler {
    private static final Logger logger = Logger.getLogger(RDBHandler.class);
    private final RedisCore redisCore;

    public RDBHandler(RedisCore redisCore) {
        this.redisCore = redisCore;
    }

    public void save() throws IOException {
        logger.info("开始保存RDB文件");
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(RDBConstants.RDB_FILE_NAME))) {
            writeRDBHeader(dos);
            saveAllDatabases(dos);
            writeRDBFooter(dos);
            logger.info("RDB文件保存成功");
        } catch (IOException e) {
            logger.error("保存RDB文件时发生错误", e);
            throw e;
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
        File file = new File(RDBConstants.RDB_FILE_NAME);
        if (!file.exists() || file.length() == 0) {
            logger.info("RDB文件不存在或为空，跳过加载过程");
            return;
        }

        logger.info("开始加载RDB文件");
        try (DataInputStream dis = new DataInputStream(new FileInputStream(file))) {
            if (!validateRDBHeader(dis)) {
                return;
            }

            clearAllDatabases();
            loadData(dis);

            logger.info("RDB文件加载成功");
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
}
