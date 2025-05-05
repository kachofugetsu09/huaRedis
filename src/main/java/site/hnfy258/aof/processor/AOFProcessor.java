package site.hnfy258.aof.processor;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.log4j.Logger;
import site.hnfy258.aof.writer.Writer;
import site.hnfy258.protocal.Resp;
import site.hnfy258.utils.DoubleBufferBlockingQueue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * AOF处理器，负责将命令追加到AOF文件
 */
public class AOFProcessor implements Processor {
    private static final Logger logger = Logger.getLogger(AOFProcessor.class);

    private final DoubleBufferBlockingQueue bufferQueue;    // 双缓冲队列
    private  Writer writer;                            // 文件写入器
    private final AtomicBoolean running;                    // 运行状态标志
    private final AtomicLong appendedBytes = new AtomicLong(0);  // 统计已追加字节数

    private final int batchSize;
    private ByteBuf batchBuffer;
    private final ReentrantLock batchLock = new ReentrantLock();
    /**
     * 构造AOF处理器
     * @param writer 文件写入器
     * @param bufferSize 缓冲区大小
     */
    public AOFProcessor(Writer writer, int bufferSize, int batchSize) {
        // 1. 初始化组件
        this.writer = writer;
        this.bufferQueue = new DoubleBufferBlockingQueue(bufferSize);
        this.running = new AtomicBoolean(true);
        this.batchSize = batchSize;
        this.batchBuffer = Unpooled.directBuffer(batchSize);
    }

    public AOFProcessor(Writer writer, int bufferSize) {
        this(writer, bufferSize, 4096); // 默认批处理大小为4KB
    }

    @Override
    public void append(Resp command) {
        // 如果不在运行状态或命令为空，不处理
        if (!running.get() || command == null) {
            return;
        }

        try {
            batchLock.lock();
            try {
                // 确保批处理缓冲区有效
                ensureValidBatchBuffer();
                
                // 估计命令大小
                int estimatedSize = estimateCommandSize(command);

                // 处理大命令，直接写入队列
                if (estimatedSize > batchSize) {
                    handleLargeCommand(command, estimatedSize);
                    return;
                }

                // 如果批处理缓冲区剩余空间不足，先刷新
                if (batchBuffer.writableBytes() < estimatedSize) {
                    // 日志记录批处理缓冲区状态
                    if (logger.isDebugEnabled()) {
                        logger.debug("批处理缓冲区空间不足: 需要 " + estimatedSize + " 字节, 剩余 " + 
                                    batchBuffer.writableBytes() + " 字节, 当前已用 " + 
                                    batchBuffer.readableBytes() + " 字节");
                    }
                    flushBatch();
                }

                // 二次检查 - 如果刷新后仍然空间不足（可能是估计大小不准确）
                if (batchBuffer.writableBytes() < estimatedSize) {
                    logger.warn("刷新后缓冲区仍然空间不足，改用大命令处理方式");
                    handleLargeCommand(command, estimatedSize);
                    return;
                }

                // 写入前记录位置
                int writerIndex = batchBuffer.writerIndex();
                
                // 尝试将命令写入批处理缓冲区
                try {
                    command.write(command, batchBuffer);
                    int bytesWritten = batchBuffer.writerIndex() - writerIndex;
                    appendedBytes.addAndGet(bytesWritten);
                    
                    // 安全检查 - 如果写入的字节数超过估计值的150%，记录警告
                    if (bytesWritten > estimatedSize * 1.5) {
                        logger.warn("命令实际大小 (" + bytesWritten + " 字节) 远大于估计大小 (" + 
                                   estimatedSize + " 字节)，请检查估计算法");
                    }
                } catch (IndexOutOfBoundsException e) {
                    // 处理写入失败 - 回滚写入位置并改用大命令处理
                    batchBuffer.writerIndex(writerIndex);
                    logger.error("写入批处理缓冲区失败，回滚并尝试大命令处理: " + e.getMessage());
                    handleLargeCommand(command, Math.max(estimatedSize * 2, batchSize));
                    return;
                }

                // 如果批处理缓冲区剩余空间不足10%，刷新
                if (batchBuffer.writableBytes() < batchSize / 10) {
                    flushBatch();
                }
            } finally {
                batchLock.unlock();
            }
        } catch (InterruptedException e) {
            logger.error("AOFProcessor.append() interrupted", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            logger.error("AOFProcessor.append() error", e);
        }
    }

    /**
     * 确保批处理缓冲区有效
     */
    private void ensureValidBatchBuffer() {
        if (batchBuffer == null || !batchBuffer.isWritable()) {
            if (batchBuffer != null) {
                logger.warn("发现无效的批处理缓冲区，释放并重新创建");
                batchBuffer.release();
            }
            batchBuffer = Unpooled.directBuffer(batchSize);
        }
    }

    /**
     * 处理大命令，直接写入队列
     */
    private void handleLargeCommand(Resp command, int estimatedSize) throws InterruptedException {
        ByteBuf buf = Unpooled.directBuffer(estimatedSize);
        try {
            command.write(command, buf);
            int actualSize = buf.readableBytes();
            appendedBytes.addAndGet(actualSize);

            ByteBuffer byteBuffer = buf.nioBuffer();
            bufferQueue.put(byteBuffer);
        } finally {
            buf.release();
        }
    }


    /**
     * 更准确地估计命令大小
     */
    private int estimateCommandSize(Resp command) {
        if (command == null) {
            return 0;
        }
        
        // 使用临时缓冲区测量命令实际大小
        ByteBuf tempBuf = Unpooled.buffer(256);
        try {
            command.write(command, tempBuf);
            int actualSize = tempBuf.readableBytes();
            
            // 添加一定的安全余量(20%)
            int estimatedSize = (int)(actualSize * 1.2);
            
            // 设置合理的最小和最大值
            return Math.max(128, Math.min(estimatedSize, batchSize));
        } catch (Exception e) {
            logger.warn("估计命令大小时出错，返回默认大小", e);
            return 1024; // 安全的默认值
        } finally {
            tempBuf.release();
        }
    }

    /**
     * 刷新批处理缓冲区
     */
    private void flushBatch() throws InterruptedException {
        // 如果缓冲区为空或无数据，直接返回
        if (batchBuffer == null || batchBuffer.readableBytes() <= 0) {
            return;
        }

        // 获取批处理的ByteBuffer并放入队列
        ByteBuffer byteBuffer = batchBuffer.nioBuffer(0, batchBuffer.readableBytes());
        bufferQueue.put(byteBuffer);

        // 释放旧缓冲区，创建新缓冲区
        batchBuffer.release();
        batchBuffer = Unpooled.directBuffer(batchSize);
    }


    @Override
    public void flush() throws IOException {
        try {
            // 刷新批处理缓冲区
            batchLock.lock();
            try {
                flushBatch();
            } finally {
                batchLock.unlock();
            }

            // 从双缓冲队列中获取待写入的缓冲区并写入文件
            ByteBuffer buffer = bufferQueue.poll();
            if (buffer != null && buffer.hasRemaining()) {
                writer.write(buffer);
            }
        } catch (InterruptedException e) {
            logger.error("AOFProcessor.flush() interrupted", e);
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            logger.error("AOFProcessor.flush() IO error", e);
            throw e;
        }
    }

    @Override
    public void stop() {
        // 停止处理器
        if (running.compareAndSet(true, false)) {
            try {
                // 确保所有批处理数据都被刷新
                batchLock.lock();
                try {
                    flushBatch();

                    // 释放资源
                    if (batchBuffer != null) {
                        batchBuffer.release();
                        batchBuffer = null;
                    }
                } finally {
                    batchLock.unlock();
                }
            } catch (Exception e) {
                logger.error("Error during AOFProcessor shutdown", e);
            }
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    /**
     * 获取已追加的字节数，用于监控
     */
    public long getAppendedBytes() {
        return appendedBytes.get();
    }

    /**
     * 获取写入位置，用于监控
     */
    public long getWritePosition() {
        return bufferQueue.getWritePosition();
    }

    /**
     * 获取刷盘位置，用于监控
     */
    public long getFlushPosition() {
        return bufferQueue.getFlushPosition();
    }

    /**
     * 获取未刷盘数据大小，用于监控
     */
    public long getUnflushedSize() {
        return bufferQueue.getUnflushedSize();
    }


    public void setWriter(Writer writer){
        this.writer = writer;
    }

}

