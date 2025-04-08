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

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class AOFHandler {
    private static final Logger logger = Logger.getLogger(AOFHandler.class);
    private final String filename;
    private RandomAccessFile raf;
    private FileChannel fileChannel;
    private final LinkedBlockingQueue<Resp> commandQueue;
    private final AtomicBoolean running;
    private Thread bgSaveThread;
    private Thread syncThread;

    private static final int BUFFER_SIZE = 2 * 1024 * 1024; // 2MB buffer
    private ByteBuffer currentBuffer;
    private ByteBuffer flushingBuffer;
    private CommandType commandType;

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
        this.currentBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
        this.flushingBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
    }

    public void start() throws IOException {
        this.raf = new RandomAccessFile(filename, "rw");
        this.fileChannel = raf.getChannel();
        raf.seek(raf.length());

        this.bgSaveThread = new Thread(this::backgroundSave);
        this.bgSaveThread.setName("aof-background-save");
        this.bgSaveThread.setDaemon(true);
        this.bgSaveThread.start();

        if (syncStrategy == AOFSyncStrategy.EVERYSEC) {
            this.syncThread = new Thread(this::backgroundSync);
            this.syncThread.setName("aof-background-sync");
            this.syncThread.setDaemon(true);
            this.syncThread.start();
        }
    }

    public void append(Resp command) {
        if (running.get()) {
            commandQueue.offer(command);
        }
    }

    private void backgroundSave() {
        while (running.get()) {
            try {
                Resp command = commandQueue.poll(100, TimeUnit.MILLISECONDS);
                if (command != null) {
                    ByteBuf buf = Unpooled.buffer();
                    command.write(command, buf);
                    int size = buf.readableBytes();

                    if (currentBuffer.remaining() < size) {
                        swapBuffers();
                    }

                    currentBuffer.put(buf.nioBuffer());

                    if (syncStrategy == AOFSyncStrategy.ALWAYS) {
                        swapBuffers();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (IOException e) {
                logger.error("Error writing to AOF file", e);
                if (running.get()) {
                    logger.error("Disabling AOF due to I/O error");
                    running.set(false);
                }
            }
        }

        try {
            swapBuffers();
            flushBuffer();
        } catch (IOException e) {
            logger.error("Error during final AOF flush", e);
        }
    }

    private void backgroundSync() {
        while (running.get()) {
            try {
                Thread.sleep(1000);
                swapBuffers();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (IOException e) {
                logger.error("Error during AOF sync", e);
            }
        }
    }

    private synchronized void swapBuffers() throws IOException {
        ByteBuffer temp = currentBuffer;
        currentBuffer = flushingBuffer;
        flushingBuffer = temp;

        flushingBuffer.flip();
        flushBuffer();
        flushingBuffer.clear();
    }

    private void flushBuffer() throws IOException {
        if (fileChannel != null && fileChannel.isOpen() && flushingBuffer.hasRemaining()) {
            fileChannel.write(flushingBuffer);
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
                // 最后一次尝试刷新
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
            ByteBuffer buffer = ByteBuffer.allocate(8192); // 8KB buffer
            ByteBuf byteBuf = Unpooled.buffer();

            int commandsLoaded = 0;
            int commandsFailed = 0;

            while (channel.read(buffer) != -1) {
                buffer.flip();
                byteBuf.writeBytes(buffer);
                buffer.clear();

                try {
                    while (byteBuf.isReadable()) {
                        // 标记当前位置，以便在命令不完整时回退
                        byteBuf.markReaderIndex();

                        try {
                            Resp command = Resp.decode(byteBuf);

                            if (command instanceof RespArray) {
                                RespArray array = (RespArray) command;
                                Resp[] params = array.getArray();

                                if (params.length > 0 && params[0] instanceof BulkString) {
                                    String commandName = ((BulkString) params[0]).getContent().toUtf8String().toUpperCase();

                                    try {
                                        CommandType commandType = CommandType.valueOf(commandName);
                                        Command cmd = commandType.getSupplier().apply(redisCore);
                                        cmd.setContext(params);
                                        cmd.handle();
                                        commandsLoaded++;

                                        if (commandsLoaded % 10000 == 0) {
                                            logger.info("已加载 " + commandsLoaded + " 条命令");
                                        }
                                    } catch (IllegalArgumentException e) {
                                        logger.warn("未知命令: " + commandName);
                                        commandsFailed++;
                                    } catch (Exception e) {
                                        logger.error("执行命令失败: " + commandName, e);
                                        commandsFailed++;
                                    }
                                }
                            }
                        } catch (IllegalStateException e) {
                            // 如果命令不完整，回退到标记位置并等待更多数据
                            byteBuf.resetReaderIndex();
                            break;
                        }
                    }

                    // 压缩缓冲区，移除已处理的数据
                    byteBuf.discardReadBytes();

                } catch (Exception e) {
                    logger.error("处理AOF数据时出错", e);
                    commandsFailed++;
                }
            }

            logger.info("AOF加载完成: 成功加载 " + commandsLoaded + " 条命令, 失败 " + commandsFailed + " 条");
        } catch (IOException e) {
            logger.error("读取AOF文件时出错", e);
            throw e;
        }
    }


    private void executeBatch(List<Command> commands) {
        for (Command cmd : commands) {
            cmd.handle();
        }
    }
}
