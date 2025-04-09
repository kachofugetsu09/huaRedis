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
                // 1. 从阻塞队列获取待持久化的Redis命令
                // 使用100ms超时避免无限阻塞，同时平衡响应速度和CPU使用率
                Resp command = commandQueue.poll(100, TimeUnit.MILLISECONDS);

                if (command != null) {
                    // 2. 序列化Redis命令
                    // 使用非池化缓冲区(每个命令独立分配，适合短生命周期对象)
                    ByteBuf buf = Unpooled.buffer();
                    command.write(command, buf);
                    int serializedSize = buf.readableBytes();

                    // 3. 检查当前缓冲区剩余空间
                    // 如果空间不足，交换缓冲区并将数据刷盘
                    if (currentBuffer.remaining() < serializedSize) {
                        swapBuffers();
                    }

                    // 4. 将序列化后的命令写入当前缓冲区
                    currentBuffer.put(buf.nioBuffer());

                    // 5. 处理ALWAYS同步策略
                    // 每次写入后立即交换缓冲区确保数据持久化
                    if (syncStrategy == AOFSyncStrategy.ALWAYS) {
                        swapBuffers();
                    }
                }
            } catch (InterruptedException e) {
                // 6. 处理线程中断
                Thread.currentThread().interrupt();
                break;
            } catch (IOException e) {
                // 7. 处理IO异常
                logger.error("AOF文件写入错误", e);
                if (running.get()) {
                    logger.error("由于IO错误禁用AOF功能");
                    running.set(false); // 优雅降级，禁用AOF但保持服务运行
                }
            }
        }

        // 8. 线程结束前的清理工作
        try {
            swapBuffers();  // 确保所有数据都交换到刷盘缓冲区
            flushBuffer();  // 最后一次写到磁盘
        } catch (IOException e) {
            logger.error("最终AOF刷盘错误", e);
        }
    }

    /**
     * 后台同步线程，按策略定期将缓冲区内容刷入磁盘
     */
    private void backgroundSync() {
        while (running.get()) {
            try {
                // 1. 每秒触发一次同步(EVERYSEC策略)
                Thread.sleep(1000);

                // 2. 交换缓冲区并写到磁盘
                swapBuffers();
            } catch (InterruptedException e) {
                // 3. 处理线程中断
                Thread.currentThread().interrupt();
                break;
            } catch (IOException e) {
                logger.error("AOF同步错误", e);
            }
        }
    }

    /**
     * 交换双缓冲区并刷盘(线程安全)
     *
     * @throws IOException 如果刷盘失败抛出IO异常
     */
    private synchronized void swapBuffers() throws IOException {
        // 1. 交换缓冲区引用(原子操作)
        // 当前缓冲区变为刷盘缓冲区，刷盘缓冲区变为当前缓冲区
        ByteBuffer temp = currentBuffer;
        currentBuffer = flushingBuffer;
        flushingBuffer = temp;

        // 2. 准备刷盘缓冲区
        // flip()操作将limit设为position，position设为0，准备读取
        flushingBuffer.flip();

        // 3. 将数据写入磁盘
        flushBuffer();

        // 4. 重置刷盘缓冲区
        // clear()操作将position设为0，limit设为capacity，准备下次写入
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

        // 1. 检查AOF文件是否存在且非空
        File file = new File(filename);
        if (!file.exists() || file.length() == 0) {
            logger.info("AOF文件不存在或为空，跳过加载");
            return;
        }

        // 2. 使用NIO方式读取文件（try-with-resources确保自动关闭）
        try (FileChannel channel = new RandomAccessFile(filename, "r").getChannel()) {
            // 3. 初始化缓冲区
            ByteBuffer buffer = ByteBuffer.allocate(8192); // 8KB的文件读取缓冲区
            ByteBuf byteBuf = Unpooled.buffer();          // Netty缓冲区用于命令解析

            int commandsLoaded = 0;  // 成功加载命令计数器
            int commandsFailed = 0;  // 失败命令计数器

            // 4. 主读取循环（从文件读取数据）
            while (channel.read(buffer) != -1) {
                // 4.1 准备读取buffer中的数据
                buffer.flip();
                // 4.2 将数据从ByteBuffer转移到Netty的ByteBuf
                byteBuf.writeBytes(buffer);
                // 4.3 清空buffer以便下次读取
                buffer.clear();

                try {
                    // 5. 命令解析循环（处理ByteBuf中的数据）
                    while (byteBuf.isReadable()) {
                        // 5.1 标记当前位置，以便解析失败时回退
                        byteBuf.markReaderIndex();

                        try {
                            // 6. 解析Redis协议格式的命令
                            Resp command = Resp.decode(byteBuf);

                            // 7. 处理数组类型的命令（Redis命令都以数组形式存储）
                            if (command instanceof RespArray) {
                                RespArray array = (RespArray) command;
                                Resp[] params = array.getArray();

                                // 7.1 验证命令格式（第一个参数必须是BulkString表示命令名）
                                if (params.length > 0 && params[0] instanceof BulkString) {
                                    String commandName = ((BulkString) params[0]).getContent()
                                            .toUtf8String().toUpperCase();

                                    try {
                                        // 8. 根据命令名创建对应的命令对象
                                        CommandType commandType = CommandType.valueOf(commandName);
                                        Command cmd = commandType.getSupplier().apply(redisCore);

                                        // 9. 设置命令参数并执行
                                        cmd.setContext(params);
                                        cmd.handle();
                                        commandsLoaded++;

                                        // 10. 每10000条命令打印进度
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
                            // 11. 处理不完整命令（回退并等待更多数据）
                            byteBuf.resetReaderIndex();
                            break;
                        }
                    }

                    // 12. 压缩缓冲区（回收已处理数据占用的空间）
                    byteBuf.discardReadBytes();

                } catch (Exception e) {
                    logger.error("处理AOF数据时出错", e);
                    commandsFailed++;
                }
            }

            // 13. 最终统计信息输出
            logger.info("AOF加载完成: 成功加载 " + commandsLoaded + " 条命令, 失败 " + commandsFailed + " 条");
        } catch (IOException e) {
            logger.error("读取AOF文件时出错", e);
            throw e;
        }
    }
}
