package site.hnfy258.aof;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.log4j.Logger;
import site.hnfy258.RedisCore;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;
import site.hnfy258.utils.DoubleBufferBlockingQueue;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class AOFHandler {
    private static final Logger logger = Logger.getLogger(AOFHandler.class);
    private final String filename;

    //支持同时读写，随机访问
    private RandomAccessFile raf;
    //NIOChannel 可以使用内存映射文件
    private FileChannel fileChannel;
    private final LinkedBlockingQueue<Resp> commandQueue;
    private final AtomicBoolean running;
    //添加命令到缓存区的线程
    private Thread bgSaveThread;
    //同步线程
    private Thread syncThread;

    private static final int BUFFER_SIZE = 2 * 1024 * 1024; // 2MB buffer


    private DoubleBufferBlockingQueue bufferQueue;


    // AOF同步策略
    public enum AOFSyncStrategy {
        ALWAYS,    // 每次写入都同步
        EVERYSEC,  // 每秒同步一次
        NO         // 由操作系统决定同步时机
    }

    private AOFSyncStrategy syncStrategy = AOFSyncStrategy.EVERYSEC;
    private long lastSyncTime = System.currentTimeMillis();

    public AOFHandler(String filename) {
        this.filename = filename;
        this.commandQueue = new LinkedBlockingQueue<>();
        this.running = new AtomicBoolean(true);

        this.bufferQueue = new DoubleBufferBlockingQueue(BUFFER_SIZE);
    }

    public void start() throws IOException {
        // 初始化文件资源，rw模式开启（文本以追加模式打开）
        this.raf = new RandomAccessFile(filename, "rw");
        this.fileChannel = raf.getChannel();
        // 设置文件指针到文件末尾
        raf.seek(raf.length());

        this.bgSaveThread = new Thread(this::backgroundSave);
        this.bgSaveThread.setName("aof-background-save");
        // 设置后台线程为守护线程，这样主线程退出时，后台线程也会退出
        this.bgSaveThread.setDaemon(true);
        this.bgSaveThread.start();

        if (syncStrategy == AOFSyncStrategy.EVERYSEC) {
            // 创建后台同步线程
            this.syncThread = new Thread(this::backgroundSync);
            this.syncThread.setName("aof-background-sync");
            this.syncThread.setDaemon(true);
            this.syncThread.start();
        }
    }
    // 添加命令到缓存区
    public void append(Resp command) {
        if (running.get()) {
            commandQueue.offer(command);
        }
    }

    /**
     * 后台持久化线程的主方法，负责将命令队列中的命令写入缓冲区
     */
    private void backgroundSave() {
        while (running.get()) {
            try {
                Resp command = commandQueue.poll(100, TimeUnit.MILLISECONDS);
                if (command != null) {
                    ByteBuf buf = Unpooled.buffer();
                    command.write(command, buf);
                    ByteBuffer byteBuffer = buf.nioBuffer();

                    bufferQueue.put(byteBuffer);

                    if (syncStrategy == AOFSyncStrategy.ALWAYS) {
                        flushBuffer();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("AOF写入错误", e);
                if (running.get()) {
                    logger.error("由于错误禁用AOF功能");
                    running.set(false);
                }
            }
        }

        try {
            flushBuffer();
        } catch (IOException e) {
            logger.error("最终AOF刷盘错误", e);
        }
    }

    private void backgroundSync() {
        while (running.get()) {
            try {
                Thread.sleep(1000);
                flushBuffer();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (IOException e) {
                logger.error("AOF同步错误", e);
            }
        }
    }

    private void flushBuffer() throws IOException {
        ByteBuffer buffer = bufferQueue.poll();
        if (buffer != null && buffer.hasRemaining()) {
            fileChannel.write(buffer);
            if (syncStrategy != AOFSyncStrategy.NO) {
                fileChannel.force(false);
            }
        }
    }

    public void stop() {
        // 首先设置running为false，通知后台线程准备退出
        running.set(false);

        // 中断后台线程，以便它能立即响应
        if (bgSaveThread != null && bgSaveThread.isAlive()) {
            bgSaveThread.interrupt();
            try {
                // 等待后台线程完成，最多等待3秒
                bgSaveThread.join(3000);
                if (bgSaveThread.isAlive()) {
                    logger.warn("AOF background thread did not terminate in time");
                } else {
                    //logger.info("AOF background thread terminated successfully");
                }
            } catch (InterruptedException e) {
                logger.warn("Interrupted while waiting for AOF background thread to stop");
                Thread.currentThread().interrupt();
            }
        }

        // 关闭文件资源
        try {
            if (fileChannel != null && fileChannel.isOpen()) {
                // 因为缓存区中可能还有未写完的数据，所以进行最后一次刷新防止有一部分数据丢失
                try {
                    flushBuffer();
                } catch (IOException e) {
                    logger.error("Error during final flush in stop method", e);
                }

                fileChannel.close();
                //logger.info("AOF file channel closed");
            }

            if (raf != null) {
                raf.close();
                //logger.info("AOF random access file closed");
            }
        } catch (IOException e) {
            logger.error("Error closing AOF resources", e);
        }
    }

    public void setSyncStrategy(AOFSyncStrategy strategy) {
        this.syncStrategy = strategy;
    }

    public void load(RedisCore redisCore) throws IOException {
        logger.info("开始加载AOF文件: " + filename);

        File file = new File(filename);
        if (!file.exists() || file.length() == 0) {
            logger.info("AOF文件不存在或为空，跳过加载");
            return;
        }

        try (FileChannel channel = new RandomAccessFile(filename, "r").getChannel()) {
            ByteBuffer buffer = ByteBuffer.allocate(8192);
            ByteBuf byteBuf = Unpooled.buffer();

            LoadStats stats = new LoadStats();
            int currentDbIndex = 0;

            while (channel.read(buffer) != -1) {
                buffer.flip();
                byteBuf.writeBytes(buffer);
                buffer.clear();

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
            byteBuf.markReaderIndex();
            try {
                Resp command = Resp.decode(byteBuf);
                if (command instanceof RespArray) {
                    RespArray array = (RespArray) command;
                    Resp[] params = array.getArray();
                    if (params.length > 0 && params[0] instanceof BulkString) {
                        String commandName = ((BulkString) params[0]).getContent().toUtf8String().toUpperCase();
                        currentDbIndex = executeCommand(redisCore, params, commandName, stats, currentDbIndex);
                    }
                }
            } catch (IllegalStateException e) {
                byteBuf.resetReaderIndex();
                break;
            } catch (Exception e) {
                logger.error("处理AOF数据时出错", e);
                stats.commandsFailed++;
            }
        }
        byteBuf.discardReadBytes();
        return currentDbIndex;
    }

    private int executeCommand(RedisCore redisCore, Resp[] params, String commandName, LoadStats stats, int currentDbIndex) {
        try {
            if (commandName.equals("SELECT")) {
                return handleSelectCommand(redisCore, params, currentDbIndex);
            } else {
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
                int dbIndex = Integer.parseInt(dbIndexStr);
                if (dbIndex >= 0 && dbIndex < redisCore.getDbNum()) {
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
        if (commandsLoaded % 10000 == 0) {
            logger.info("已加载 " + commandsLoaded + " 条命令");
        }
    }

    private static class LoadStats {
        int commandsLoaded = 0;
        int commandsFailed = 0;
    }
}
