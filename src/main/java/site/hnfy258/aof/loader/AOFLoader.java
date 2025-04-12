package site.hnfy258.aof.loader;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.log4j.Logger;
import site.hnfy258.RedisCore;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class AOFLoader implements Loader {
    private static final Logger logger = Logger.getLogger(AOFLoader.class);

    @Override
    public void load(String filename, RedisCore redisCore) throws IOException {
        logger.info("开始加载AOF文件: " + filename);

        File file = new File(filename);
        if (!file.exists() || file.length() == 0) {
            logger.info("AOF文件不存在或为空，跳过加载");
            return;
        }

        try (FileChannel channel = new RandomAccessFile(filename, "r").getChannel()) {
            // 1. 创建缓冲区
            ByteBuffer buffer = ByteBuffer.allocate(8192);  // 一级缓存
            ByteBuf byteBuf = Unpooled.buffer();  // 二级缓存

            LoadStats stats = new LoadStats();
            int currentDbIndex = 0;

            // 2. 读取文件内容
            while (channel.read(buffer) != -1) {
                buffer.flip();  // 切换为读模式
                byteBuf.writeBytes(buffer);  // 写入到二级缓存中
                buffer.clear();  // 清空一级缓存

                // 3. 处理读取到的命令
                currentDbIndex = processCommands(redisCore, byteBuf, stats, currentDbIndex);
            }

            logger.info("AOF加载完成: 成功加载 " + stats.commandsLoaded + " 条命令, 失败 " + stats.commandsFailed + " 条");
        } catch (IOException e) {
            logger.error("读取AOF文件时出错", e);
            throw e;
        }
    }

    private int processCommands(RedisCore redisCore, ByteBuf byteBuf, LoadStats stats, int currentDbIndex) {
        while (byteBuf.isReadable()) {
            byteBuf.markReaderIndex();  // 保存当前读取位置
            try {
                // 1. 解码命令
                Resp command = Resp.decode(byteBuf);
                if (command instanceof RespArray) {
                    RespArray array = (RespArray) command;
                    Resp[] params = array.getArray();
                    if (params.length > 0 && params[0] instanceof BulkString) {
                        String commandName = ((BulkString) params[0]).getContent().toUtf8String().toUpperCase();
                        // 2. 执行命令
                        currentDbIndex = executeCommand(redisCore, params, commandName, stats, currentDbIndex);
                    }
                }
            } catch (IllegalStateException e) {
                byteBuf.resetReaderIndex();  // 返回上次保存的位置
                break;
            } catch (Exception e) {
                logger.error("处理AOF数据时出错", e);
                stats.commandsFailed++;
            }
        }
        byteBuf.discardReadBytes();  // 清除已读取的数据
        return currentDbIndex;
    }

    private int executeCommand(RedisCore redisCore, Resp[] params, String commandName, LoadStats stats, int currentDbIndex) {
        try {
            // 1. 处理SELECT命令
            if (commandName.equals("SELECT")) {
                return handleSelectCommand(redisCore, params, currentDbIndex);
            } else {
                // 2. 执行其他命令
                redisCore.selectDB(currentDbIndex);
                CommandType commandType = CommandType.valueOf(commandName);
                Command cmd = commandType.getSupplier().apply(redisCore);
                cmd.setContext(params);
                cmd.handle();
                stats.commandsLoaded++;
                logProgress(stats.commandsLoaded);
            }
        } catch (IllegalArgumentException e) {
            logger.warn("未知命令: " + commandName);
            stats.commandsFailed++;
        } catch (Exception e) {
            logger.error("执行命令失败: " + commandName, e);
            stats.commandsFailed++;
        }
        return currentDbIndex;
    }

    private int handleSelectCommand(RedisCore redisCore, Resp[] params, int currentDbIndex) {
        if (params.length > 1 && params[1] instanceof BulkString) {
            String dbIndexStr = ((BulkString) params[1]).getContent().toUtf8String();
            try {
                // 1. 解析数据库索引
                int dbIndex = Integer.parseInt(dbIndexStr);
                if (dbIndex >= 0 && dbIndex < redisCore.getDbNum()) {
                    // 2. 切换数据库
                    currentDbIndex = dbIndex;
                    redisCore.selectDB(currentDbIndex);
                    logger.debug("切换到数据库: " + currentDbIndex);
                } else {
                    logger.warn("无效的数据库索引: " + dbIndex);
                }
            } catch (NumberFormatException e) {
                logger.warn("无效的数据库索引格式: " + dbIndexStr);
            }
        }
        return currentDbIndex;
    }

    private void logProgress(int commandsLoaded) {
        // 每处理10000条命令记录一次日志
        if (commandsLoaded % 10000 == 0) {
            logger.info("已加载 " + commandsLoaded + " 条命令");
        }
    }
}
