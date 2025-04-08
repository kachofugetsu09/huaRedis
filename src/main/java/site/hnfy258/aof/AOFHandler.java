package site.hnfy258.aof;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.log4j.Logger;
import site.hnfy258.protocal.Resp;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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
    private long currentPosition;

    private static final int BUFFER_SIZE = 2 * 1024 * 1024; // 2MB buffer
    private ByteBuffer buffer;

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
        this.currentPosition = 0;
        this.buffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
    }

    public void start() throws IOException {
        this.raf = new RandomAccessFile(filename, "rw");
        this.fileChannel = raf.getChannel();
        this.currentPosition = raf.length();
        raf.seek(currentPosition);

        this.bgSaveThread = new Thread(this::backgroundSave);
        this.bgSaveThread.setName("aof-background-save");
        this.bgSaveThread.setDaemon(true); // 设置为守护线程，这样主程序退出时不会阻塞
        this.bgSaveThread.start();
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

                    if (buffer.remaining() < size) {
                        flushBuffer();
                    }

                    buffer.put(buf.nioBuffer());

                    if (syncStrategy == AOFSyncStrategy.ALWAYS) {
                        flushBuffer();
                    } else if (syncStrategy == AOFSyncStrategy.EVERYSEC) {
                        long now = System.currentTimeMillis();
                        if (now - lastSyncTime >= 1000) {
                            flushBuffer();
                            lastSyncTime = now;
                        }
                    }
                } else {
                    // 如果队列为空，也要检查是否需要同步
                    if (syncStrategy == AOFSyncStrategy.EVERYSEC) {
                        long now = System.currentTimeMillis();
                        if (now - lastSyncTime >= 1000 && buffer.position() > 0) {
                            flushBuffer();
                            lastSyncTime = now;
                        }
                    }
                }
            } catch (InterruptedException e) {
                // 线程被中断，可能是服务正在关闭
                //logger.info("AOF background save thread interrupted, preparing to exit");
                Thread.currentThread().interrupt(); // 重新设置中断标志
                break; // 退出循环
            } catch (IOException e) {
                logger.error("Error writing to AOF file", e);
                // 如果发生IO错误，我们可能需要停止AOF
                if (running.get()) {
                    logger.error("Disabling AOF due to I/O error");
                    running.set(false);
                }
            }
        }

        // 确保在退出前刷新所有数据
        try {
            if (buffer != null && buffer.position() > 0) {
                flushBuffer();
                //logger.info("Final AOF buffer flush completed");
            }
        } catch (IOException e) {
            logger.error("Error during final AOF flush", e);
        }
    }

    private void flushBuffer() throws IOException {
        if (fileChannel != null && fileChannel.isOpen() && buffer.position() > 0) {
            buffer.flip();
            fileChannel.write(buffer);
            buffer.clear();
            if (syncStrategy != AOFSyncStrategy.NO) {
                fileChannel.force(false);
            }
            currentPosition = fileChannel.position();
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
}
