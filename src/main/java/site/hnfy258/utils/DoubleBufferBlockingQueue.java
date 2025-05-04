package site.hnfy258.utils;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.Logger;

public class DoubleBufferBlockingQueue implements BlockingQueue<ByteBuffer> {
    private static final Logger logger = Logger.getLogger(DoubleBufferBlockingQueue.class);

    // 缓冲区配置 - 核心参数
    private static final int INITIAL_BUFFER_SIZE = 1 << 21; // 初始缓冲区大小: 2MB (2^21 bytes)
    private static final int MIN_BUFFER_SIZE = 1 << 20;     // 最小缓冲区大小: 1MB (2^20 bytes)
    private static final int MAX_BUFFER_SIZE = 1 << 26;     // 最大缓冲区大小: 64MB (2^26 bytes)
    private static final float HIGH_WATER_MARK = 0.75f;     // 高水位线(触发扩容)
    private static final float LOW_WATER_MARK = 0.25f;      // 低水位线(触发缩容)

    // 性能指标
    private final AtomicInteger resizeCount = new AtomicInteger(0);    // 调整大小计数
    private final AtomicInteger overflowCount = new AtomicInteger(0);  // 溢出计数
    private final AtomicLong totalBytesWritten = new AtomicLong(0);    // 写入总字节数
    private final AtomicLong totalFlushedBytes = new AtomicLong(0);    // 刷盘总字节数
    private volatile long lastFlushTime = System.currentTimeMillis();  // 上次刷盘时间
    private volatile long lastResizeTime = System.currentTimeMillis(); // 上次调整大小时间

    // 当前活动配置
    private volatile int bufferSize;
    private ByteBuffer currentBuffer;
    private ByteBuffer flushingBuffer;

    // 锁和条件变量
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notFull = lock.newCondition();
    private final Condition notEmpty = lock.newCondition();

    // 队列状态
    private volatile boolean closed = false;
    private volatile long writePosition = 0;
    private volatile long flushPosition = 0;

    // 小数据处理优化
    private static final int SMALL_DATA_THRESHOLD = 1024;   // 小数据阈值(字节)
    private static final int MAX_SMALL_DATA_QUEUE_SIZE = 10 * 1024 * 1024; // 10MB
    private final ConcurrentLinkedQueue<ByteBuffer> smallDataQueue = new ConcurrentLinkedQueue<>();
    private final AtomicInteger smallDataQueueSize = new AtomicInteger(0);
    private final AtomicInteger smallDataCount = new AtomicInteger(0);

    // 批处理配置
    private static final int INITIAL_BATCH_SIZE = 64;
    private static final int MAX_BATCH_SIZE = 128;
    private volatile int currentBatchSize = INITIAL_BATCH_SIZE;
    private final AtomicInteger batchCounter = new AtomicInteger(0);
    private volatile long lastBatchTime = System.currentTimeMillis();

    // 构造函数
    public DoubleBufferBlockingQueue() {
        this(INITIAL_BUFFER_SIZE);
    }

    public DoubleBufferBlockingQueue(int initialBufferSize) {
        this.bufferSize = Math.min(Math.max(initialBufferSize, MIN_BUFFER_SIZE), MAX_BUFFER_SIZE);
        this.currentBuffer = ByteBuffer.allocateDirect(bufferSize);
        this.flushingBuffer = ByteBuffer.allocateDirect(bufferSize);
    }

    /**
     * 处理超大数据（大于最大缓冲区大小）
     */
    private void handleOversizedData(ByteBuffer src) throws InterruptedException {
        int requiredSpace = src.remaining();
        overflowCount.incrementAndGet();

        logger.debug("处理超大数据: " + requiredSpace + " 字节");

        // 分片处理大数据
        ByteBuffer srcCopy = src.duplicate(); // 复制引用避免修改原始位置
        int bytesRemaining = requiredSpace;
        int chunkSize = Math.min(MAX_BUFFER_SIZE / 4, 4 * 1024 * 1024); // 减小分片大小，提高安全性

        while (bytesRemaining > 0) {
            // 计算当前分片大小
            int currentChunkSize = Math.min(bytesRemaining, chunkSize);

            lock.lock();
            try {
                // 如果当前缓冲区有数据且空间不足，先交换缓冲区
                if (currentBuffer.position() > 0 && currentBuffer.remaining() < currentChunkSize) {
                    swapBuffers();
                    notEmpty.signal();
                }

                // 确保当前缓冲区有足够空间
                if (currentBuffer.remaining() < currentChunkSize) {
                    // 如果新缓冲区仍然不够大，则调整大小
                    int newSize = calculateOptimalBufferSize(currentChunkSize + currentBuffer.position());
                    if (newSize > bufferSize) {
                        bufferSize = newSize;
                        resizeCount.incrementAndGet();
                        ByteBuffer newBuffer = ByteBuffer.allocateDirect(bufferSize);

                        // 复制现有数据到新缓冲区
                        if (currentBuffer.position() > 0) {
                            currentBuffer.flip();
                            newBuffer.put(currentBuffer);
                            currentBuffer.clear();
                        }

                        // 释放旧缓冲区，使用新缓冲区
                        currentBuffer = newBuffer;
                        logger.debug("调整缓冲区大小以容纳大数据分片: " + bufferSize + " 字节");
                    }
                }

                // 限制源缓冲区读取量，确保不超过当前缓冲区剩余空间
                int originalLimit = srcCopy.limit();
                int safeChunkSize = Math.min(currentChunkSize, currentBuffer.remaining());
                srcCopy.limit(srcCopy.position() + safeChunkSize);

                // 写入分片数据
                try {
                    int beforePos = currentBuffer.position();
                    currentBuffer.put(srcCopy);
                    int bytesWritten = currentBuffer.position() - beforePos;
                    writePosition += bytesWritten;
                    bytesRemaining -= bytesWritten; // 更新实际写入的字节数

                    logger.debug("成功写入大数据分片: " + bytesWritten + " 字节, 剩余 " + bytesRemaining + " 字节");
                } catch (Exception e) {
                    logger.error("写入大数据分片失败: " + e.getMessage() +
                              ", 源缓冲区: remaining=" + srcCopy.remaining() +
                              ", 目标缓冲区: remaining=" + currentBuffer.remaining() +
                              ", position=" + currentBuffer.position() +
                              ", capacity=" + currentBuffer.capacity(), e);
                    throw e;
                } finally {
                    // 恢复源缓冲区限制
                    srcCopy.limit(originalLimit);
                }
            } finally {
                lock.unlock();
            }
            
            // 如果没有进展，等待一下并重新尝试
            if (bytesRemaining >= requiredSpace) {
                logger.warn("处理大数据无进展，等待一下再重试。总字节: " + requiredSpace + ", 剩余: " + bytesRemaining);
                Thread.sleep(10); // 短暂等待
            }
        }
    }

    @Override
    public void put(ByteBuffer src) throws InterruptedException {
        if (closed) {
            throw new IllegalStateException("Queue is closed");
        }

        int requiredSpace = src.remaining();

        // 更新吞吐量统计
        totalBytesWritten.addAndGet(requiredSpace);

        // 小数据处理优化路径
        if (requiredSpace <= SMALL_DATA_THRESHOLD) {
            handleSmallData(src);
            return;
        }

        // 特殊处理：处理超大数据情况，超过最大缓冲区大小
        if (requiredSpace > MAX_BUFFER_SIZE) {
            handleOversizedData(src);
            return;
        }

        lock.lock();
        try {
            // 如果有累积的小数据，先处理它们
            flushSmallDataQueueIfNeeded(false);

            // 如果当前缓冲区空间不足，等待或调整大小
            while (currentBuffer.remaining() < requiredSpace) {
                // 如果当前缓冲区存在数据且需要更大的空间
                if (currentBuffer.position() > 0) {
                    // 先交换缓冲区
                    swapBuffers();
                    notEmpty.signal();

                    // 如果新缓冲区仍然不够大，则调整大小
                    if (currentBuffer.capacity() < requiredSpace) {
                        resizeCurrentBuffer(requiredSpace);
                    }
                } else {
                    // 当前缓冲区为空但容量不足，直接调整大小
                    if (requiredSpace <= MAX_BUFFER_SIZE) {
                        resizeCurrentBuffer(requiredSpace);
                    } else {
                        // 如果请求空间太大，等待空间可用
                        notFull.await();
                    }
                }

                if (closed) {
                    throw new IllegalStateException("Queue is closed");
                }
            }

            // 执行数据写入
            int beforePos = currentBuffer.position();
            currentBuffer.put(src);
            writePosition += (currentBuffer.position() - beforePos);

            // 检查是否需要动态调整缓冲区大小
            checkAndResizeBuffer();

            // 通知可能有数据可读
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 调整当前缓冲区大小以容纳所需空间
     */
    private void resizeCurrentBuffer(int requiredSpace) {
        if (requiredSpace <= 0) {
            return; // 防止无效的请求
        }
        
        int newSize = calculateOptimalBufferSize(requiredSpace);

        if (newSize > bufferSize) {
            logger.debug("调整缓冲区大小: 从 " + bufferSize + " 字节 到 " + newSize + " 字节");
            bufferSize = newSize;
            resizeCount.incrementAndGet();
            ByteBuffer newBuffer = ByteBuffer.allocateDirect(bufferSize);

            // 复制现有数据到新缓冲区
            if (currentBuffer != null && currentBuffer.position() > 0) {
                currentBuffer.flip();
                newBuffer.put(currentBuffer);
            }

            // 释放旧缓冲区，使用新缓冲区
            currentBuffer = newBuffer;
            lastResizeTime = System.currentTimeMillis();
        }
    }

    /**
     * 计算最佳缓冲区大小
     */
    private int calculateOptimalBufferSize(int requiredSpace) {
        // 确保至少是所需空间的1.5倍，避免频繁调整
        int minSize = (int)(requiredSpace * 1.5);

        // 找到大于minSize的最小2的幂
        int newSize = MIN_BUFFER_SIZE;
        while (newSize < minSize && newSize < MAX_BUFFER_SIZE) {
            newSize <<= 1; // 翻倍
        }

        return Math.min(newSize, MAX_BUFFER_SIZE);
    }


    private void handleSmallData(ByteBuffer src) throws InterruptedException {
        if (src.remaining() <= 0) return;

        // 创建小数据副本并添加到队列
        ByteBuffer copy = ByteBuffer.allocate(src.remaining());
        copy.put(src.duplicate());
        copy.flip();

        // 使用无锁并发队列添加小数据
        smallDataQueue.offer(copy);
        int queueSize = smallDataQueueSize.addAndGet(copy.capacity());
        smallDataCount.incrementAndGet();

        // 批量处理条件
        if (queueSize >= MAX_SMALL_DATA_QUEUE_SIZE/2 ||
                smallDataCount.get() >= currentBatchSize ||
                System.currentTimeMillis() - lastBatchTime > 5) {
            lock.lock();
            try {
                flushSmallDataQueueIfNeeded(true);
            } finally {
                lock.unlock();
            }
        }

        // 动态调整批处理大小
        if (smallDataCount.get() % 100 == 0) {
            adjustBatchSize();
        }
    }

    /**
     * 刷新小数据队列到主缓冲区
     */
    private void flushSmallDataQueueIfNeeded(boolean force) {
        int count = smallDataCount.get();
        int queueSize = smallDataQueueSize.get();

        if (count == 0 || queueSize == 0) {
            return;
        }

        boolean shouldFlush = force ||
                count >= currentBatchSize ||
                queueSize >= MAX_SMALL_DATA_QUEUE_SIZE/2 ||
                System.currentTimeMillis() - lastBatchTime > 10;

        if (!shouldFlush) {
            return;
        }

        // 计算需要的总空间
        int totalRequired = Math.min(queueSize, MAX_SMALL_DATA_QUEUE_SIZE);

        // 如果当前缓冲区空间不足，交换缓冲区
        if (currentBuffer.remaining() < totalRequired && currentBuffer.position() > 0) {
            swapBuffers();
            notEmpty.signal();
        }

        // 如果新缓冲区仍然空间不足，调整大小
        if (currentBuffer.remaining() < totalRequired) {
            resizeCurrentBuffer(totalRequired + currentBuffer.position());
        }

        // 批量处理队列中的数据
        int processedCount = 0;
        int processedBytes = 0;
        batchCounter.incrementAndGet();

        ByteBuffer item;
        while ((item = smallDataQueue.poll()) != null && processedBytes < MAX_SMALL_DATA_QUEUE_SIZE) {
            // 确保当前缓冲区有足够空间存放这个小数据
            if (currentBuffer.remaining() < item.remaining()) {
                // 如果当前项太大，先处理已有数据，然后交换缓冲区或调整大小
                if (currentBuffer.position() > 0) {
                    swapBuffers();
                    notEmpty.signal();
                }
                
                // 如果新缓冲区仍然不够大，调整大小
                if (currentBuffer.remaining() < item.remaining()) {
                    resizeCurrentBuffer(item.remaining() + currentBuffer.position());
                }
            }
            
            try {
                int beforePos = currentBuffer.position();
                currentBuffer.put(item);
                int bytesWritten = currentBuffer.position() - beforePos;

                writePosition += bytesWritten;
                processedBytes += bytesWritten;
                processedCount++;
            } catch (Exception e) {
                logger.error("添加小数据到主缓冲区出错: " + e.getMessage() +
                          ", 小数据大小: " + item.remaining() +
                          ", 主缓冲区剩余: " + currentBuffer.remaining(), e);
                
                // 将这个项放回队列
                smallDataQueue.offer(item);
                break;
            }

            if (processedCount >= currentBatchSize * 2) {
                break;
            }
        }

        // 更新计数器
        if (processedCount > 0) {
            smallDataCount.addAndGet(-processedCount);
            smallDataQueueSize.addAndGet(-processedBytes);
            lastBatchTime = System.currentTimeMillis();
        }
    }

    /**
     * 动态调整批处理大小
     */
    private void adjustBatchSize() {
        long now = System.currentTimeMillis();
        long timeDiff = now - lastBatchTime;

        // 根据时间间隔动态调整批大小
        if (timeDiff < 5 && currentBatchSize < MAX_BATCH_SIZE) {
            currentBatchSize = Math.min(currentBatchSize + 8, MAX_BATCH_SIZE);
        } else if (timeDiff > 20 && currentBatchSize > INITIAL_BATCH_SIZE) {
            currentBatchSize = Math.max(currentBatchSize - 4, INITIAL_BATCH_SIZE);
        }
    }

    @Override
    public ByteBuffer take() throws InterruptedException {
        lock.lock();
        try {
            // 先尝试刷新小数据队列
            flushSmallDataQueueIfNeeded(false);

            while (currentBuffer.position() == 0 && !closed) {
                // 再次检查小数据队列
                if (smallDataCount.get() > 0) {
                    flushSmallDataQueueIfNeeded(true);
                    if (currentBuffer.position() > 0) {
                        break;
                    }
                }
                notEmpty.await();
            }

            if (closed && currentBuffer.position() == 0) {
                return null;
            }

            swapBuffers();

            // 更新刷盘统计
            totalFlushedBytes.addAndGet(flushingBuffer.remaining());
            lastFlushTime = System.currentTimeMillis();

            notFull.signal();
            return flushingBuffer;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 交换写入缓冲区和刷新缓冲区
     */
    private void swapBuffers() {
        if (currentBuffer.position() == 0) {
            return;  // 无数据，不需要交换
        }

        // 准备当前缓冲区用于刷盘
        currentBuffer.flip();

        // 交换缓冲区（零拷贝）
        ByteBuffer temp = flushingBuffer;
        flushingBuffer = currentBuffer;

        // 记录刷盘位置
        flushPosition = writePosition;

        // 准备新的写入缓冲区
        if (temp.capacity() != bufferSize) {
            // 如果大小不匹配，创建新的合适大小的缓冲区
            currentBuffer = ByteBuffer.allocateDirect(bufferSize);
        } else {
            // 重用之前的缓冲区（零拷贝）
            temp.clear();
            currentBuffer = temp;
        }
    }

    /**
     * 检查并动态调整缓冲区大小
     */
    private void checkAndResizeBuffer() {
        long now = System.currentTimeMillis();

        // 限制调整频率
        if (now - lastResizeTime < 3000) {
            return;
        }

        float usageRatio = (float) currentBuffer.position() / currentBuffer.capacity();

        // 计算写入速率（字节/毫秒）
        long timeElapsed = Math.max(1, now - lastFlushTime);
        float writeRate = (float) totalBytesWritten.get() / timeElapsed;

        // 预测2秒内需要的缓冲区大小
        int predictedSize = (int) (writeRate * 2000);

        // 检查是否需要扩容
        if ((usageRatio > HIGH_WATER_MARK || predictedSize > bufferSize * HIGH_WATER_MARK) &&
                bufferSize < MAX_BUFFER_SIZE) {

            // 计算新的缓冲区大小
            int newSize = bufferSize << 1; // 扩大为两倍
            newSize = Math.min(newSize, MAX_BUFFER_SIZE);

            // 确保能容纳预测数据
            if (newSize < predictedSize && newSize < MAX_BUFFER_SIZE) {
                newSize = calculateOptimalBufferSize(predictedSize);
            }

            if (newSize > bufferSize) {
                bufferSize = newSize;
                resizeCount.incrementAndGet();
                lastResizeTime = now;
            }
        }
        // 检查是否需要缩容
        else if (usageRatio < LOW_WATER_MARK &&
                 bufferSize > (MIN_BUFFER_SIZE << 1) &&
                 now - lastResizeTime > 30000 &&
                 predictedSize < bufferSize * LOW_WATER_MARK) {

            int newSize = bufferSize >> 1; // 缩小为一半
            newSize = Math.max(newSize, MIN_BUFFER_SIZE);

            if (newSize < bufferSize) {
                bufferSize = newSize;
                resizeCount.incrementAndGet();
                lastResizeTime = now;
            }
        }
    }

    public void close() {
        lock.lock();
        try {
            if (!closed) {
                closed = true;
                notEmpty.signalAll();
                notFull.signalAll();

                logger.info(String.format("关闭双缓冲队列: 总写入=%d bytes, 总刷盘=%d bytes, 调整次数=%d, 溢出次数=%d",
                        totalBytesWritten.get(), totalFlushedBytes.get(),
                        resizeCount.get(), overflowCount.get()));
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean offer(ByteBuffer byteBuffer) {
        if (closed) {
            return false;
        }

        lock.lock();
        try {
            int requiredSpace = byteBuffer.remaining();

            // 大数据在非阻塞模式下不支持
            if (requiredSpace > MAX_BUFFER_SIZE) {
                return false;
            }

            if (currentBuffer.remaining() < requiredSpace) {
                return false;
            }

            int beforePos = currentBuffer.position();
            currentBuffer.put(byteBuffer);
            writePosition += (currentBuffer.position() - beforePos);

            checkAndResizeBuffer();
            notEmpty.signal();
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean offer(ByteBuffer byteBuffer, long timeout, TimeUnit unit) throws InterruptedException {
        if (closed) {
            return false;
        }

        long nanos = unit.toNanos(timeout);

        // 特殊处理：处理大数据情况，超过最大缓冲区大小
        int requiredSpace = byteBuffer.remaining();
        if (requiredSpace > MAX_BUFFER_SIZE) {
            handleOversizedData(byteBuffer);
            return true;
        }

        lock.lock();
        try {
            // 等待空间可用
            while (currentBuffer.remaining() < requiredSpace) {
                if (nanos <= 0) {
                    return false;
                }

                if (currentBuffer.position() > 0) {
                    swapBuffers();
                    notEmpty.signal();

                    if (currentBuffer.remaining() >= requiredSpace) {
                        break;
                    }
                }

                nanos = notFull.awaitNanos(nanos);

                if (closed) {
                    return false;
                }
            }

            int beforePos = currentBuffer.position();
            currentBuffer.put(byteBuffer);
            writePosition += (currentBuffer.position() - beforePos);

            checkAndResizeBuffer();
            notEmpty.signal();
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public ByteBuffer poll() {
        lock.lock();
        try {
            if (currentBuffer.position() == 0) {
                return null;
            }

            swapBuffers();
            totalFlushedBytes.addAndGet(flushingBuffer.remaining());

            notFull.signal();
            return flushingBuffer.duplicate();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public ByteBuffer poll(long timeout, TimeUnit unit) throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        lock.lock();
        try {
            while (currentBuffer.position() == 0 && !closed) {
                if (nanos <= 0) {
                    return null;
                }
                nanos = notEmpty.awaitNanos(nanos);
            }

            if (closed && currentBuffer.position() == 0) {
                return null;
            }

            swapBuffers();
            totalFlushedBytes.addAndGet(flushingBuffer.remaining());

            notFull.signal();
            return flushingBuffer.duplicate();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int remainingCapacity() {
        lock.lock();
        try {
            return currentBuffer.remaining();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int drainTo(Collection<? super ByteBuffer> c) {
        return drainTo(c, Integer.MAX_VALUE);
    }

    @Override
    public int drainTo(Collection<? super ByteBuffer> c, int maxElements) {
        if (c == null) throw new NullPointerException();
        if (c == this) throw new IllegalArgumentException();
        if (maxElements <= 0) return 0;

        lock.lock();
        try {
            int n = 0;

            if (currentBuffer.position() > 0) {
                swapBuffers();
                c.add(flushingBuffer.duplicate());
                totalFlushedBytes.addAndGet(flushingBuffer.remaining());

                notFull.signal();
                n = 1;
            }

            return n;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int size() {
        lock.lock();
        try {
            return currentBuffer.position();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean isEmpty() {
        lock.lock();
        try {
            return currentBuffer.position() == 0;
        } finally {
            lock.unlock();
        }
    }

    // 以下方法实现 Collection<ByteBuffer> 接口，但不是核心功能

    @Override public boolean contains(Object o) { return false; }
    @Override public Iterator<ByteBuffer> iterator() { throw new UnsupportedOperationException(); }
    @Override public Object[] toArray() { throw new UnsupportedOperationException(); }
    @Override public <T> T[] toArray(T[] a) { throw new UnsupportedOperationException(); }
    @Override public boolean remove(Object o) { return false; }
    @Override public boolean containsAll(Collection<?> c) { return false; }
    @Override public boolean addAll(Collection<? extends ByteBuffer> c) { throw new UnsupportedOperationException(); }
    @Override public boolean removeAll(Collection<?> c) { return false; }
    @Override public boolean retainAll(Collection<?> c) { return false; }

    @Override
    public void clear() {
        lock.lock();
        try {
            currentBuffer.clear();
            flushingBuffer.clear();
            notFull.signalAll();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean add(ByteBuffer buffer) {
        if (offer(buffer)) {
            return true;
        }
        throw new IllegalStateException("Queue full");
    }

    @Override
    public ByteBuffer remove() {
        ByteBuffer buffer = poll();
        if (buffer == null) {
            throw new IllegalStateException("Queue empty");
        }
        return buffer;
    }

    @Override
    public ByteBuffer element() {
        ByteBuffer buffer = peek();
        if (buffer == null) {
            throw new IllegalStateException("Queue empty");
        }
        return buffer;
    }

    @Override
    public ByteBuffer peek() {
        lock.lock();
        try {
            if (currentBuffer.position() == 0) {
                return null;
            }
            // 返回当前缓冲区的只读视图
            ByteBuffer view = currentBuffer.duplicate();
            view.flip();
            return view;
        } finally {
            lock.unlock();
        }
    }

    // 辅助方法

    public long getWritePosition() {
        return writePosition;
    }

    public long getFlushPosition() {
        return flushPosition;
    }

    public long getUnflushedSize() {
        return writePosition - flushPosition;
    }

    public long getTotalFlushedBytes() {
        return totalFlushedBytes.get();
    }

    public String getPerformanceStats() {
        return String.format(
                "总写入: %d字节, 总刷新: %d字节, 缓冲区调整: %d次, 溢出: %d次, 批处理: %d次, 当前批次大小: %d",
                totalBytesWritten.get(), totalFlushedBytes.get(),
                resizeCount.get(), overflowCount.get(),
                batchCounter.get(), currentBatchSize
        );
    }
}