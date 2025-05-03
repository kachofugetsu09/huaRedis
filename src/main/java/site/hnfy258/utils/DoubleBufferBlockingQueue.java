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


    // 简化的性能指标
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

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notFull = lock.newCondition();
    private final Condition notEmpty = lock.newCondition();

    private volatile boolean closed = false;

    private volatile long writePosition = 0;
    private volatile long flushPosition = 0;


    // 新增的类成员变量
    private static final int INITIAL_BATCH_SIZE = 64;       // 初始批量大小
    private static final int MAX_BATCH_SIZE = 128;          // 最大批量大小
    private volatile int currentBatchSize = INITIAL_BATCH_SIZE;

    // 小数据缓存队列 - 用于优化小数据处理
    private final ConcurrentLinkedQueue<ByteBuffer> smallDataQueue = new ConcurrentLinkedQueue<>();
    private final AtomicInteger smallDataQueueSize = new AtomicInteger(0);
    private final AtomicInteger smallDataCount = new AtomicInteger(0);
    private static final int SMALL_DATA_THRESHOLD = 1024;   // 小数据阈值(字节)
    private static final int MAX_SMALL_DATA_QUEUE_SIZE = 10 * 1024 * 1024; // 10MB

    // 批量计数器
    private final AtomicInteger batchCounter = new AtomicInteger(0);
    private volatile long lastBatchTime = System.currentTimeMillis();

    private static final int SEGMENT_COUNT = 8;
    private final ReentrantLock[] segmentLocks = new ReentrantLock[SEGMENT_COUNT];
    private final Condition[] segmentNotFull = new Condition[SEGMENT_COUNT];
    private ByteBuffer[] bufferSegments = new ByteBuffer[SEGMENT_COUNT];
    private final AtomicInteger segmentSelector = new AtomicInteger(0);

    // 构造函数
    public DoubleBufferBlockingQueue() {
        this(INITIAL_BUFFER_SIZE);
    }

    public DoubleBufferBlockingQueue(int initialBufferSize) {
        this.bufferSize = Math.min(Math.max(initialBufferSize, MIN_BUFFER_SIZE), MAX_BUFFER_SIZE);
        this.currentBuffer = ByteBuffer.allocateDirect(bufferSize);
        this.flushingBuffer = ByteBuffer.allocateDirect(bufferSize);

        // 初始化分段锁
        for (int i = 0; i < SEGMENT_COUNT; i++) {
            segmentLocks[i] = new ReentrantLock();
            segmentNotFull[i] = segmentLocks[i].newCondition();
            bufferSegments[i] = ByteBuffer.allocateDirect(bufferSize / SEGMENT_COUNT);
        }
    }



    /**
     * 处理超大数据（大于最大缓冲区大小）
     */
    private void handleOversizedData(ByteBuffer src) throws InterruptedException {
        int requiredSpace = src.remaining();
        overflowCount.incrementAndGet();

        // 分片处理大数据
        ByteBuffer srcCopy = src.duplicate(); // 复制引用避免修改原始位置
        int bytesRemaining = requiredSpace;


        int chunkSize = Math.min(MAX_BUFFER_SIZE >> 1, 8 << 20);

        // 在高并发下使用独立缓冲区来减少锁争用
        boolean highConcurrency = Thread.activeCount() > 8;
        ByteBuffer localBuffer = null;

        if (highConcurrency && requiredSpace > bufferSize / 2) {
            // 对于极端场景，直接分配临时缓冲区而不是使用共享缓冲区
            localBuffer = ByteBuffer.allocateDirect(Math.min(requiredSpace, MAX_BUFFER_SIZE));
            chunkSize = localBuffer.capacity();
        }

        while (bytesRemaining > 0) {
            // 计算当前分片大小
            int currentChunkSize = Math.min(bytesRemaining, chunkSize);

            if (localBuffer != null) {
                // 使用本地临时缓冲区，减少锁争用
                int originalLimit = srcCopy.limit();
                srcCopy.limit(srcCopy.position() + currentChunkSize);

                // 在临时缓冲区外获取锁，减少持锁时间
                localBuffer.clear();
                localBuffer.put(srcCopy);
                localBuffer.flip();

                // 只在必要时获取锁
                lock.lock();
                try {
                    // 如果当前缓冲区有数据且空间不足，先刷盘
                    if (currentBuffer.position() > 0 && currentBuffer.remaining() < localBuffer.remaining()) {
                        swapArea();
                        notEmpty.signal();
                    }

                    // 写入主缓冲区
                    int beforePos = currentBuffer.position();
                    currentBuffer.put(localBuffer);
                    writePosition += (currentBuffer.position() - beforePos);
                } finally {
                    lock.unlock();
                }

                // 恢复源缓冲区限制
                srcCopy.limit(originalLimit);
            } else {
                // 使用常规路径处理
                lock.lock();
                try {
                    // 如果当前缓冲区有数据且空间不足，先刷盘
                    if (currentBuffer.position() > 0 && currentBuffer.remaining() < currentChunkSize) {
                        swapArea();
                        notEmpty.signal();
                    }

                    // 限制源缓冲区读取量
                    int originalLimit = srcCopy.limit();
                    srcCopy.limit(srcCopy.position() + currentChunkSize);

                    // 写入分片数据
                    int beforePos = currentBuffer.position();
                    currentBuffer.put(srcCopy);
                    writePosition += (currentBuffer.position() - beforePos);

                    // 恢复源缓冲区限制
                    srcCopy.limit(originalLimit);
                } finally {
                    lock.unlock();
                }
            }

            // 更新剩余字节数
            bytesRemaining -= currentChunkSize;
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

        lock.lock();
        try {
            // 如果有累积的小数据，先处理它们
            flushSmallDataQueueIfNeeded(false);

            // 特殊处理：处理超大数据情况，超过最大缓冲区大小
            if (requiredSpace > MAX_BUFFER_SIZE) {
                handleOversizedData(src);
                return;
            }

            // 如果当前缓冲区空间不足，等待刷新或动态调整大小
            while (currentBuffer.remaining() < requiredSpace) {
                // 如果当前缓冲区存在数据且需要更大的空间
                if (currentBuffer.position() > 0) {
                    // 主动触发交换
                    swapArea();
                    notEmpty.signal();

                    // 检查新缓冲区是否够大，不够则调整大小
                    if (currentBuffer.capacity() < requiredSpace && bufferSize < requiredSpace) {
                        // 调整缓冲区大小以适应大数据
                        int newSize = Math.min(
                                Math.max(requiredSpace, (int) (1<<bufferSize)),
                                MAX_BUFFER_SIZE
                        );

//                        logger.info(String.format(
//                                "数据较大，动态调整缓冲区: 当前大小=%d, 新大小=%d, 请求空间=%d",
//                                bufferSize, newSize, requiredSpace
//                        ));

                        bufferSize = newSize;
                        resizeCount.incrementAndGet();

                        // 创建新的更大缓冲区
                        currentBuffer = ByteBuffer.allocateDirect(bufferSize);
                    }
                } else {
                    // 没有数据但缓冲区仍然不足，说明请求的数据太大
                    // 尝试调整缓冲区大小直接适应
                    if (requiredSpace <= MAX_BUFFER_SIZE) {
                        int newSize = Math.min(
                                Math.max(requiredSpace, (int) (1<<bufferSize )),
                                MAX_BUFFER_SIZE
                        );

//                        logger.info(String.format(
//                                "缓冲区不足，动态调整: 当前大小=%d, 新大小=%d, 请求空间=%d",
//                                bufferSize, newSize, requiredSpace
//                        ));

                        bufferSize = newSize;
                        resizeCount.incrementAndGet();

                        // 创建新的更大缓冲区
                        currentBuffer = ByteBuffer.allocateDirect(bufferSize);
                    } else {
                        // 如果请求空间太大，等待空间可用
                        notFull.await();
                    }
                }

                if (closed) {
                    throw new IllegalStateException("Queue is closed");
                }
            }

            // 记录写入前的位置，用于跟踪
            int beforePos = currentBuffer.position();

            // 执行数据写入
            currentBuffer.put(src);

            // 更新写入位置
            writePosition += (currentBuffer.position() - beforePos);

            // 在每次写入后检查是否需要调整缓冲区大小
            resizeBufferIfNeeded();

            // 通知可能有数据可读
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    private void handleSmallData(ByteBuffer src) throws InterruptedException {
        if (src.remaining() <= 0) return;

        // 创建小数据副本并添加到队列
        ByteBuffer copy = ByteBuffer.allocate(src.remaining());
        copy.put(src);
        copy.flip();

        // 使用无锁并发队列添加小数据
        smallDataQueue.offer(copy);
        int queueSize = smallDataQueueSize.addAndGet(copy.capacity());
        smallDataCount.incrementAndGet();

        // 批量处理条件：队列大小超过阈值或数量达到批处理大小
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

        // 如果队列为空，直接返回
        if (count == 0 || queueSize == 0) {
            return;
        }

        // 决定是否需要刷新
        boolean shouldFlush = force || count >= currentBatchSize ||
                queueSize >= MAX_SMALL_DATA_QUEUE_SIZE/2 ||
                System.currentTimeMillis() - lastBatchTime > 10;

        if (!shouldFlush) {
            return;
        }

        // 计算需要的总空间
        int totalRequired = Math.min(queueSize, MAX_SMALL_DATA_QUEUE_SIZE);

        // 如果当前缓冲区空间不足，触发交换
        if (currentBuffer.remaining() < totalRequired && currentBuffer.position() > 0) {
            swapArea();
            notEmpty.signal();
        }

        // 如果新缓冲区仍然空间不足，调整大小
        if (currentBuffer.remaining() < totalRequired) {
            int newSize = Math.min(
                    Math.max(totalRequired + currentBuffer.position(), (int)(1<<bufferSize)),
                    MAX_BUFFER_SIZE
            );

            if (newSize > bufferSize) {
                bufferSize = newSize;
                resizeCount.incrementAndGet();

                // 创建新缓冲区并复制现有数据
                ByteBuffer newBuffer = ByteBuffer.allocateDirect(bufferSize);
                currentBuffer.flip();
                newBuffer.put(currentBuffer);
                currentBuffer = newBuffer;
            }
        }

        // 批量处理队列中的数据
        int processedCount = 0;
        int processedBytes = 0;

        ByteBuffer item;
        while ((item = smallDataQueue.poll()) != null && processedBytes < MAX_SMALL_DATA_QUEUE_SIZE) {
            int beforePos = currentBuffer.position();
            currentBuffer.put(item);
            int bytesWritten = currentBuffer.position() - beforePos;

            // 更新写入位置
            writePosition += bytesWritten;
            processedBytes += bytesWritten;
            processedCount++;

            // 避免一次处理太多导致延迟过高
            if (processedCount >= currentBatchSize * 2) {
                break;
            }
        }

        // 更新计数器
        smallDataCount.addAndGet(-processedCount);
        smallDataQueueSize.addAndGet(-processedBytes);
        lastBatchTime = System.currentTimeMillis();
    }

    /**
     * 动态调整批处理大小
     */
    private void adjustBatchSize() {
        long now = System.currentTimeMillis();
        long timeDiff = now - lastBatchTime;

        // 时间间隔过短，增加批处理大小
        if (timeDiff < 5 && currentBatchSize < MAX_BATCH_SIZE) {
            currentBatchSize = Math.min(currentBatchSize + 8, MAX_BATCH_SIZE);
        }
        // 时间间隔过长，减少批处理大小
        else if (timeDiff > 20 && currentBatchSize > INITIAL_BATCH_SIZE) {
            currentBatchSize = Math.max(currentBatchSize - 4, INITIAL_BATCH_SIZE);
        }
    }

    @Override
    public ByteBuffer take() throws InterruptedException {
        lock.lock();
        try {
            // 首先尝试刷新小数据队列 - 提高响应性
            flushSmallDataQueueIfNeeded(false);

            while (currentBuffer.position() == 0 && !closed) {
                // 再次检查小数据队列 - 避免可能的竞争条件
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

            swapArea();

            notFull.signal();
            return flushingBuffer;
        } finally {
            lock.unlock();
        }
    }

    private void swapArea() {
        if (currentBuffer.position() == 0) {
            // 无数据，不需要交换
            return;
        }

        // 准备当前缓冲区用于刷盘
        currentBuffer.flip();

        // 交换缓冲区
        ByteBuffer temp = flushingBuffer;
        flushingBuffer = currentBuffer;

        // 记录刷盘位置
        flushPosition = writePosition;

        // 准备新的当前缓冲区 - 优化：重用或创建更合适大小的缓冲区
        if (temp.capacity() != bufferSize) {
            // 如果大小不匹配，创建新的合适大小的缓冲区
            currentBuffer = ByteBuffer.allocateDirect(bufferSize);
        } else {
            // 重用之前的缓冲区
            temp.clear();
            currentBuffer = temp;
        }

        // 通知可能在等待的读取线程
        notEmpty.signalAll();
    }

    /**
     * 改进的动态缓冲区大小调整
     */
    private void resizeBufferIfNeeded() {
        long now = System.currentTimeMillis();

        // 限制调整频率
        if (now - lastResizeTime < 3000) { // 3秒间隔
            return;
        }

        // 计算使用率
        float usageRatio = (float) currentBuffer.position() / currentBuffer.capacity();

        // 计算数据增长率
        long timeElapsed = Math.max(1, now - lastFlushTime);
        float dataRate = (float) totalBytesWritten.get() / timeElapsed; // 字节/毫秒

        // 根据数据速率预测所需缓冲区大小
        int predictedSize = (int) (dataRate * 2000); // 预测2秒数据量

        // 使用位运算优化扩容判断 - 检查是否超过75%阈值(近似于0.75)
        // 使用右移2位来计算capacity的3/4，更高效
        boolean needExpand = currentBuffer.position() > (currentBuffer.capacity() >> 2) * 3 ||
                predictedSize > (bufferSize >> 1) + (bufferSize >> 2); // 约为0.75倍bufferSize

        // 检查是否需要扩容
        if (needExpand && bufferSize < MAX_BUFFER_SIZE) {
            // 使用位移运算计算新的缓冲区大小(下一个2的幂)
            int newSize = bufferSize << 1; // 扩大为当前的2倍

            // 确保不超过最大限制
            newSize = Math.min(newSize, MAX_BUFFER_SIZE);

            // 确保至少能容纳预测数据量
            if (newSize < predictedSize && newSize < MAX_BUFFER_SIZE) {
                // 找到大于predictedSize的最小2的幂
                newSize = 1 << (32 - Integer.numberOfLeadingZeros(predictedSize - 1));
                newSize = Math.min(newSize, MAX_BUFFER_SIZE);
            }

            if (newSize > bufferSize) {
                bufferSize = newSize;
                resizeCount.incrementAndGet();
                lastResizeTime = now;
            }
        }
        // 检查是否需要缩容 - 使用位运算优化
        // 使用右移3位计算capacity的1/8，用于判断低使用率(约为0.125)
        else if (usageRatio < 0.25 &&
                bufferSize > (MIN_BUFFER_SIZE << 1) &&
                now - lastResizeTime > 30000 &&
                predictedSize < (bufferSize >> 2)) { // 小于buffer的1/4

            // 使用位移运算缩小为当前的一半
            int newSize = bufferSize >> 1;

            // 确保不小于最小缓冲区大小
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
                        totalBytesWritten.get(), totalFlushedBytes.get(), resizeCount.get(), overflowCount.get()));
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

            // 特殊处理：处理大数据情况
            if (requiredSpace > MAX_BUFFER_SIZE) {
                // 大数据处理在非阻塞模式下不支持
                return false;
            }

            if (currentBuffer.remaining() < byteBuffer.remaining()) {
                return false;
            }
            currentBuffer.put(byteBuffer);

            // 更新写入位置
            writePosition += byteBuffer.remaining();

            // 每次写入后检查是否需要调整缓冲区大小
            resizeBufferIfNeeded();

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
        lock.lock();
        try {
            int requiredSpace = byteBuffer.remaining();

            // 特殊处理：处理大数据情况
            if (requiredSpace > MAX_BUFFER_SIZE) {
                try {
                    handleOversizedData(byteBuffer);
                    return true;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }

            while (currentBuffer.remaining() < byteBuffer.remaining()) {
                if (nanos <= 0) {
                    return false;
                }
                nanos = notFull.awaitNanos(nanos);
                if (closed) {
                    return false;
                }
            }
            currentBuffer.put(byteBuffer);

            // 更新写入位置
            writePosition += byteBuffer.remaining();

            // 每次写入后检查是否需要调整缓冲区大小
            resizeBufferIfNeeded();

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

            // 交换缓冲区
            swapArea();

            // 创建一个副本返回，防止外部修改影响内部状态
            ByteBuffer result = ByteBuffer.allocate(flushingBuffer.remaining());
            result.put(flushingBuffer.duplicate());
            result.flip();

            notFull.signal();
            return result;
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

            // 交换缓冲区
            swapArea();

            // 创建一个副本返回，防止外部修改影响内部状态
            ByteBuffer result = ByteBuffer.allocate(flushingBuffer.remaining());
            result.put(flushingBuffer.duplicate());
            result.flip();

            notFull.signal();
            return result;
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
                // 交换缓冲区
                swapArea();

                // 创建一个副本返回，防止外部修改影响内部状态
                ByteBuffer result = ByteBuffer.allocate(flushingBuffer.remaining());
                result.put(flushingBuffer.duplicate());
                result.flip();

                c.add(result);
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

    @Override
    public boolean contains(Object o) {
        return false; // 不支持此操作
    }

    @Override
    public Iterator<ByteBuffer> iterator() {
        throw new UnsupportedOperationException(); // 不支持此操作
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException(); // 不支持此操作
    }

    @Override
    public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException(); // 不支持此操作
    }

    @Override
    public boolean remove(Object o) {
        return false; // 不支持此操作
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return false; // 不支持此操作
    }

    @Override
    public boolean addAll(Collection<? extends ByteBuffer> c) {
        throw new UnsupportedOperationException(); // 不支持此操作
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return false; // 不支持此操作
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return false; // 不支持此操作
    }

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
    public boolean add(ByteBuffer byteBuffer) {
        if (offer(byteBuffer)) {
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

    public long getWritePosition() {
        return writePosition;
    }

    /**
     * 获取最后刷盘位置
     */
    public long getFlushPosition() {
        return flushPosition;
    }

    /**
     * 获取未刷盘数据大小
     */
    public long getUnflushedSize() {
        return writePosition - flushPosition;
    }

    /**
     * 获取已刷盘的总字节数
     *
     * @return 已刷盘的总字节数
     */
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
