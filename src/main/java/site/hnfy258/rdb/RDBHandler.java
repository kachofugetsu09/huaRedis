package site.hnfy258.rdb;

import site.hnfy258.RedisCore;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.datatype.RedisData;
import site.hnfy258.datatype.RedisList;
import site.hnfy258.datatype.RedisString;

import java.io.*;
import java.nio.charset.StandardCharsets;
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
            // 写入RDB文件头
            dos.writeBytes("REDIS0001");

            // 遍历所有数据库
            for (int dbIndex = 0; dbIndex < redisCore.getDbNum(); dbIndex++) {
                Map<BytesWrapper, RedisData> dbData = redisCore.getDBData(dbIndex);
                if (dbData.isEmpty()) {
                    continue; // 跳过空数据库
                }

                // 写入SELECTDB操作码
                dos.writeByte(RDBConstants.RDB_OPCODE_SELECTDB);
                // 写入数据库编号
                writeLength(dos, dbIndex);

                // 遍历当前数据库的所有键值对
                for (Map.Entry<BytesWrapper, RedisData> entry : dbData.entrySet()) {
                    BytesWrapper key = entry.getKey();
                    RedisData value = entry.getValue();

                    if (value instanceof RedisString) {
                        // 写入类型标记
                        dos.writeByte(RDBConstants.STRING_TYPE);

                        // 写入键
                        writeString(dos, key.getBytes());

                        // 写入值
                        writeString(dos, ((RedisString) value).getValue().getBytes());
                    }

                    if (value instanceof RedisString) {
                        // Existing string handling code
                        dos.writeByte(RDBConstants.STRING_TYPE);
                        writeString(dos, key.getBytes());
                        writeString(dos, ((RedisString) value).getValue().getBytes());
                    } else if (value instanceof RedisList) {
                        // New list handling code
                        dos.writeByte(RDBConstants.LIST_TYPE);

                        // Write key
                        writeString(dos, key.getBytes());

                        // Write list size
                        RedisList list = (RedisList) value;
                        int listSize = list.size();
                        writeLength(dos, listSize);

                        // Write each list element
                        for (BytesWrapper element : list.getAllElements()) {
                            writeString(dos, element.getBytes());
                        }
                    }

                    // 这里可以添加其他数据类型的处理
                }
            }

            // 写入EOF标记
            dos.writeByte(RDBConstants.RDB_OPCODE_EOF);

            // 写入CRC64校验和(简化版，实际Redis有更复杂的实现)
            dos.writeLong(0); // 实际应用中应该计算并写入正确的校验和

            logger.info("RDB文件保存成功");
        } catch (IOException e) {
            logger.error("保存RDB文件时发生错误", e);
            throw e;
        }
    }

    public void load() throws IOException {
        File file = new File(RDBConstants.RDB_FILE_NAME);
        if (!file.exists() || file.length() == 0) {
            logger.info("RDB文件不存在或为空，跳过加载过程");
            return;
        }

        logger.info("开始加载RDB文件");
        try (DataInputStream dis = new DataInputStream(new FileInputStream(file))) {
            // 读取并验证RDB文件头
            byte[] header = new byte[9];
            int bytesRead = dis.read(header);
            if (bytesRead != 9 || !"REDIS0001".equals(new String(header, StandardCharsets.UTF_8))) {
                logger.warn("无效的RDB文件格式");
                return;
            }

            // 先清空所有数据库
            for (int i = 0; i < redisCore.getDbNum(); i++) {
                redisCore.getDBData(i).clear();
            }

            int currentDb = 0; // 默认数据库0

            while (true) {
                int type = dis.read();
                if (type == -1) {
                    break;
                }

                // 先处理EOF标记
                if ((byte)type == RDBConstants.RDB_OPCODE_EOF) {
                    // 读取并忽略校验和(简化版)
                    dis.readLong();
                    logger.info("RDB文件加载成功");
                    return;
                }

                // 然后处理其他类型
                switch ((byte)type) {
                    case RDBConstants.RDB_OPCODE_SELECTDB:
                        currentDb = (int) readLength(dis);
                        logger.debug("切换到数据库: " + currentDb);
                        break;
                    case RDBConstants.STRING_TYPE:
                        BytesWrapper key = new BytesWrapper(readString(dis));
                        BytesWrapper value = new BytesWrapper(readString(dis));
                        redisCore.setDB(currentDb, key, new RedisString(value));
                        logger.debug("加载键值对到数据库" + currentDb + ": " + key);
                        break;
                    case RDBConstants.LIST_TYPE:
                        BytesWrapper listKey = new BytesWrapper(readString(dis));
                        long listSize = readLength(dis);

                        RedisList redisList = new RedisList();
                        for (int i = 0; i < listSize; i++) {
                            BytesWrapper element = new BytesWrapper(readString(dis));
                            redisList.rpush(element);
                        }

                        redisCore.setDB(currentDb, listKey, redisList);
                        logger.debug("加载列表到数据库" + currentDb + ": " + listKey + " (元素数: " + listSize + ")");
                        break;
                    case RDBConstants.RDB_OPCODE_EXPIRETIME:
                    case RDBConstants.RDB_OPCODE_EXPIRETIME_MS:
                        logger.warn("暂不支持过期时间，跳过");
                        // 跳过过期时间和实际数据
                        dis.readInt(); // 过期时间
                        continue; // 继续读取实际数据类型
                    default:
                        logger.warn("未知的数据类型: " + type);
                        return; // 遇到未知类型时停止加载
                }
            }
        } catch (EOFException e) {
            logger.error("RDB文件读取时遇到意外的文件结束", e);
        } catch (IOException e) {
            logger.error("加载RDB文件时发生错误", e);
            throw e;
        }
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

    // Redis RDB文件使用特殊编码来存储长度
    private void writeLength(DataOutputStream dos, long length) throws IOException {
        dos.writeInt((int) length);
    }

    private long readLength(DataInputStream dis) throws IOException {
        return dis.readInt() & 0xFFFFFFFFL;
    }
}