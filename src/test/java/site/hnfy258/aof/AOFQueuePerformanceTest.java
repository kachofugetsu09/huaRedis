package site.hnfy258.aof;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.log4j.Logger;
import site.hnfy258.aof.writer.Writer;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;
import site.hnfy258.utils.DoubleBufferBlockingQueue;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * AOF队列性能测试类
 * 比较JDK原生阻塞队列和双缓冲队列在AOF场景下的性能差异
 */
public class AOFQueuePerformanceTest {
    private static final Logger logger = Logger.getLogger(AOFQueuePerformanceTest.class);

    // 测试配置
    private static final int WARM_UP_SECONDS = 5;
    private static final int TEST_DURATION_SECONDS = 10;
    private static final int THREAD_COUNT = 4;
    private static final int BUFFER_SIZE = 2 * 1024 * 1024; // 2MB
    private static final int BATCH_SIZE = 1024*16; // 降低到1KB
    private static final String JDK_AOF_FILE = "test_jdk_queue.aof";
    private static final String DOUBLE_BUFFER_AOF_FILE = "test_double_buffer_queue.aof";

    // 测试状态
    private static final AtomicInteger completedCommands = new AtomicInteger(0);
    private static final AtomicLong totalLatency = new AtomicLong(0);
    private static final CountDownLatch testCompleteLatch = new CountDownLatch(1);
    private static volatile boolean stopTest = false;

    // 添加缺少的常量
    private static final AtomicLong totalBytesWritten = new AtomicLong(0);
    private static final double LARGE_VALUE_RATIO = 0.1; // 大数据比例，10%的SET命令将使用大数据
    private static final int LARGE_VALUE_SIZE = 100 * 1024; // 大数据大小为100KB

    // Add these constants
    private static final int TEST_ITERATIONS = 1;
    private static final List<TestResult> jdkResults = new ArrayList<>();
    private static final List<TestResult> doubleBufferResults = new ArrayList<>();

    // 新增的极限测试参数
    private static final int HIGH_CONCURRENCY_THREAD_COUNT = 16; // 高并发线程数
    private static final int LARGE_DATA_SIZE = 100 * 1024; // 大数据量 100KB
    private static final int HIGH_FLUSH_RATE_MS = 10; // 高频刷盘间隔 (毫秒)

    /**
     * 使用JDK原生阻塞队列的AOF处理器
     */
    static class JdkQueueAOFProcessor {
        private final BlockingQueue<ByteBuffer> queue;
        private final Writer writer;
        private final AtomicBoolean running;
        private final int batchSize;
        private ByteBuf batchBuffer;
        private final ReentrantLock batchLock = new ReentrantLock();
        
        // 添加统计字节的计数器，与双缓冲队列保持一致
        private final AtomicLong appendedBytes = new AtomicLong(0);

        public JdkQueueAOFProcessor(Writer writer, int bufferSize, int batchSize) {
            this.writer = writer;
            this.queue = new LinkedBlockingQueue<>();
            this.running = new AtomicBoolean(true);
            this.batchSize = batchSize;
            this.batchBuffer = Unpooled.directBuffer(batchSize);
        }

        public void append(Resp command) {
            if (!running.get()) {
                return;
            }

            try {
                batchLock.lock();
                try {
                    // 估计命令大小
                    int estimatedSize = 128; // 简化估计

                    if (estimatedSize > batchSize) {
                        // 命令太大，单独处理
                        ByteBuf buf = Unpooled.directBuffer(estimatedSize);
                        command.write(command, buf);
                        int actualSize = buf.readableBytes();
                        
                        // 更新统计信息
                        appendedBytes.addAndGet(actualSize);
                        logger.debug("添加大命令: 大小=" + actualSize + " 字节, 累计=" + appendedBytes.get() + " 字节");
                        
                        ByteBuffer byteBuffer = buf.nioBuffer();
                        queue.put(byteBuffer);
                        buf.release();
                        return;
                    }

                    // 如果批处理缓冲区剩余空间不足，先刷新
                    if (batchBuffer.writableBytes() < estimatedSize) {
                        flushBatch();
                    }

                    // 记录写入前的可读字节数
                    int beforeSize = batchBuffer.readableBytes();
                    
                    // 将命令写入批处理缓冲区
                    command.write(command, batchBuffer);
                    
                    // 记录写入后的可读字节数，计算实际写入量
                    int afterSize = batchBuffer.readableBytes();
                    int writtenSize = afterSize - beforeSize;
                    
                    // 更新统计信息
                    appendedBytes.addAndGet(writtenSize);

                    // 如果批处理缓冲区已满，刷新
                    if (batchBuffer.writableBytes() < batchSize / 10) {
                        flushBatch();
                    }
                } finally {
                    batchLock.unlock();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                logger.error("JdkQueueAOFProcessor.append() error", e);
            }
        }

        private void flushBatch() throws InterruptedException {
            if (batchBuffer == null || batchBuffer.readableBytes() <= 0) {
                return;
            }

            // 获取批处理的ByteBuffer并放入队列
            ByteBuffer byteBuffer = batchBuffer.nioBuffer(0, batchBuffer.readableBytes());
            // 创建一个新的缓冲区副本，避免在释放原始缓冲区后出现问题
            ByteBuffer copyBuffer = ByteBuffer.allocateDirect(byteBuffer.remaining());
            copyBuffer.put(byteBuffer.duplicate());
            copyBuffer.flip();
            queue.put(copyBuffer);

            // 释放旧缓冲区，创建新缓冲区
            batchBuffer.release();
            batchBuffer = Unpooled.directBuffer(batchSize);
        }

        public void flush() throws IOException {
            try {
                batchLock.lock();
                try {
                    flushBatch();
                } finally {
                    batchLock.unlock();
                }

                ByteBuffer buffer = queue.poll();
                if (buffer != null && buffer.hasRemaining() && writer != null) {
                    try {
                        // 确保缓冲区有效并且能被正确读取
                        if (buffer.isDirect() || buffer.hasArray()) {
                            // 记录日志以便调试
//                            logger.info(String.format(
//                                "JDK队列刷盘: 准备写入字节: %d, 位置: %d, 限制: %d, 容量: %d",
//                                buffer.remaining(), buffer.position(), buffer.limit(), buffer.capacity()));
//
                            // 保存刷盘前的字节计数，用于验证写入效果
                            long beforeBytes = totalBytesWritten.get();
                            
                            // 执行写入
                            writer.write(buffer);
                            
                            // 验证写入后的字节计数
                            long afterBytes = totalBytesWritten.get();
                            long deltaBytes = afterBytes - beforeBytes;
                            
                            // 记录日志，确认写入是否成功
//                            logger.info(String.format(
//                                "JDK队列刷盘完成: 预期写入 %d 字节, 实际增加 %d 字节, 总计 %d 字节",
//                                buffer.remaining(), deltaBytes, afterBytes));
                            
                            if (deltaBytes <= 0) {
                                logger.warn("JDK队列刷盘未增加字节计数，可能写入失败");
                            }
                        } else {
                            logger.warn("跳过无效的缓冲区");
                        }
                    } catch (Exception e) {
                        logger.error("写入文件时出错: " + e.getMessage(), e);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("线程被中断", e);
            } catch (Exception e) {
                throw new IOException("刷盘时出错: " + e.getMessage(), e);
            }
        }

        public void stop() {
            if (running.compareAndSet(true, false)) {
                try {
                    batchLock.lock();
                    try {
                        flushBatch();
                    } finally {
                        batchLock.unlock();
                    }

                    if (batchBuffer != null) {
                        batchBuffer.release();
                        batchBuffer = null;
                    }
                } catch (Exception e) {
                    logger.error("Error during JdkQueueAOFProcessor shutdown", e);
                }
            }
        }

        public boolean isRunning() {
            return running.get();
        }
        
        // 获取通过append方法统计的字节数，与双缓冲队列保持一致
        public long getAppendedBytes() {
            return appendedBytes.get();
        }
    }

    /**
     * 使用双缓冲队列的AOF处理器
     */
    static class DoubleBufferAOFProcessor {
        public final DoubleBufferBlockingQueue bufferQueue;
        private final Writer writer;
        private final AtomicBoolean running;
        private final int batchSize;
        private ByteBuf batchBuffer;
        private final ReentrantLock batchLock = new ReentrantLock();
        
        // 添加统计字节的计数器
        private final AtomicLong appendedBytes = new AtomicLong(0);

        public DoubleBufferAOFProcessor(Writer writer, int bufferSize, int batchSize) {
            this.writer = writer;
            this.bufferQueue = new DoubleBufferBlockingQueue(bufferSize);
            this.running = new AtomicBoolean(true);
            this.batchSize = batchSize;
            this.batchBuffer = Unpooled.directBuffer(batchSize);
        }

        public void append(Resp command) {
            if (!running.get()) {
                return;
            }

            try {
                batchLock.lock();
                try {
                    // 估计命令大小
                    int estimatedSize = 128; // 简化估计

                    if (estimatedSize > batchSize) {
                        // 命令太大，单独处理
                        ByteBuf buf = Unpooled.directBuffer(estimatedSize);
                        command.write(command, buf);
                        int actualSize = buf.readableBytes();
                        
                        // 更新统计信息
                        appendedBytes.addAndGet(actualSize);
                        logger.info("添加大命令: 大小=" + actualSize + " 字节, 累计=" + appendedBytes.get() + " 字节");
                        
                        ByteBuffer byteBuffer = buf.nioBuffer();
                        bufferQueue.put(byteBuffer);
                        buf.release();
                        return;
                    }

                    // 如果批处理缓冲区剩余空间不足，先刷新
                    if (batchBuffer.writableBytes() < estimatedSize) {
                        flushBatch();
                    }

                    // 记录写入前的可读字节数
                    int beforeSize = batchBuffer.readableBytes();
                    
                    // 将命令写入批处理缓冲区
                    command.write(command, batchBuffer);
                    
                    // 记录写入后的可读字节数，计算实际写入量
                    int afterSize = batchBuffer.readableBytes();
                    int writtenSize = afterSize - beforeSize;
                    
                    // 更新统计信息
                    appendedBytes.addAndGet(writtenSize);
//                    logger.info("添加命令到批处理缓冲区: 大小=" + writtenSize + " 字节, 累计=" + appendedBytes.get() + " 字节");

                    // 如果批处理缓冲区已满，刷新
                    if (batchBuffer.writableBytes() < batchSize / 10) {
                        flushBatch();
                    }
                } finally {
                    batchLock.unlock();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                logger.error("DoubleBufferAOFProcessor.append() error", e);
            }
        }

        private void flushBatch() throws InterruptedException {
            if (batchBuffer == null || batchBuffer.readableBytes() <= 0) {
                return;
            }

            // 获取批处理的ByteBuffer并放入队列
            ByteBuffer byteBuffer = batchBuffer.nioBuffer(0, batchBuffer.readableBytes());
            // 创建一个新的缓冲区副本，避免在释放原始缓冲区后出现问题
            ByteBuffer copyBuffer = ByteBuffer.allocateDirect(byteBuffer.remaining());
            copyBuffer.put(byteBuffer.duplicate());
            copyBuffer.flip();
            bufferQueue.put(copyBuffer);

            // 释放旧缓冲区，创建新缓冲区
            batchBuffer.release();
            batchBuffer = Unpooled.directBuffer(batchSize);
        }

        public void flush() throws IOException {
            try {
                batchLock.lock();
                try {
                    flushBatch();
                } finally {
                    batchLock.unlock();
                }

                ByteBuffer buffer = bufferQueue.poll();
                if (buffer != null && buffer.hasRemaining() && writer != null) {
                    try {
                        // 确保缓冲区有效并且能被正确读取
                        if (buffer.isDirect() || buffer.hasArray()) {
                            // 记录日志以便调试
//                            logger.info(String.format(
//                                "双缓冲队列刷盘: 准备写入字节: %d, 位置: %d, 限制: %d, 容量: %d",
//                                buffer.remaining(), buffer.position(), buffer.limit(), buffer.capacity()));
//
                            // 保存刷盘前的字节计数，用于验证写入效果
                            long beforeBytes = totalBytesWritten.get();
                            long queueFlushedBefore = bufferQueue.getTotalFlushedBytes();
                            
                            // 执行写入
                            writer.write(buffer);
                            
                            // 验证写入后的字节计数
                            long afterBytes = totalBytesWritten.get();
                            long deltaBytes = afterBytes - beforeBytes;
                            long queueFlushedAfter = bufferQueue.getTotalFlushedBytes();
                            long queueDeltaBytes = queueFlushedAfter - queueFlushedBefore;
                            
                            // 记录日志，确认写入是否成功
//                            logger.info(String.format(
//                                "双缓冲队列刷盘完成: 预期写入 %d 字节, 实际增加 %d 字节, 队列记录增加 %d 字节, 总计 %d 字节, 队列总计 %d 字节",
//                                buffer.remaining(), deltaBytes, queueDeltaBytes, afterBytes, queueFlushedAfter));
                            
                            if (deltaBytes <= 0) {
                                logger.warn("双缓冲队列刷盘未增加字节计数，可能写入失败");
                            }
                        } else {
                            logger.warn("跳过无效的缓冲区");
                        }
                    } catch (Exception e) {
                        logger.error("写入文件时出错: " + e.getMessage(), e);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("线程被中断", e);
            } catch (Exception e) {
                throw new IOException("刷盘时出错: " + e.getMessage(), e);
            }
        }

        public void stop() {
            if (running.compareAndSet(true, false)) {
                try {
                    batchLock.lock();
                    try {
                        flushBatch();
                    } finally {
                        batchLock.unlock();
                    }

                    if (batchBuffer != null) {
                        batchBuffer.release();
                        batchBuffer = null;
                    }
                } catch (Exception e) {
                    logger.error("Error during DoubleBufferAOFProcessor shutdown", e);
                }
            }
        }

        public boolean isRunning() {
            return running.get();
        }

        // 获取通过append方法统计的字节数
        public long getAppendedBytes() {
            return appendedBytes.get();
        }
    }

    /**
     * 测试用的文件写入器
     */
    static class TestWriter implements Writer {
        private final FileChannel channel;
        private final RandomAccessFile raf;
        private final AtomicLong bytesWritten = new AtomicLong(0);
        private final String filename;

        public TestWriter(String filename) throws IOException {
            this.filename = filename;
            File file = new File(filename);
            if (file.exists()) {
                file.delete();
            }
            this.raf = new RandomAccessFile(filename, "rw");
            this.channel = raf.getChannel();
        }

        @Override
        public void write(ByteBuffer buffer) throws IOException {
            if (buffer != null && buffer.hasRemaining()) {
                // 先保存原始的remaining值作为写入字节数
                int bytesToWrite = buffer.remaining();
                
                // 创建一个新的缓冲区副本，防止原缓冲区被释放或状态改变
                ByteBuffer copyBuffer = ByteBuffer.allocateDirect(bytesToWrite);
                // 使用duplicate避免影响原buffer的position
                copyBuffer.put(buffer.duplicate());
                copyBuffer.flip();
                
                try {
                    // 写入文件并更新字节计数
                    int written = channel.write(copyBuffer);
                    bytesWritten.addAndGet(written);
                    totalBytesWritten.addAndGet(written);
                    
                    // 添加详细日志，记录写入情况
                    logger.debug(String.format(
                        "写入文件 %s: 期望写入 %d 字节, 实际写入 %d 字节, 文件总写入量 %d 字节, 全局总写入量 %d 字节",
                        filename, bytesToWrite, written, bytesWritten.get(), totalBytesWritten.get()));
                    
                    // 记录日志以进行调试
                    if (written != bytesToWrite) {
                        logger.warn(String.format(
                            "写入字节数不匹配: 期望 %d, 实际 %d, 文件: %s", 
                            bytesToWrite, written, filename));
                    }
                } catch (Exception e) {
                    logger.error("写入文件时发生错误: " + e.getMessage(), e);
                } finally {
                    // 确保直接缓冲区被正确清理
                    if (copyBuffer.isDirect()) {
                        copyBuffer = null;
                    }
                }
            }
        }

        @Override
        public void close() {
            try {
                if (channel != null && channel.isOpen()) {
                    channel.close();
                }
                if (raf != null) {
                    raf.close();
                }
            } catch (IOException e) {
                logger.error("关闭资源时出错", e);
            }
        }

        @Override
        public void force() throws IOException {
            if (channel != null && channel.isOpen()) {
                channel.force(false);
            }
        }

        public long getBytesWritten() {
            return bytesWritten.get();
        }

        public String getFilename() {
            return filename;
        }
    }

    /**
     * 后台刷盘服务
     */
    static class BackgroundFlushService {
        private final Thread flushThread;
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final int flushIntervalMs;
        private final Object flushLock = new Object(); // 添加锁对象，确保刷盘操作的同步

        public BackgroundFlushService(Runnable flushTask, int flushIntervalMs) {
            this.flushIntervalMs = flushIntervalMs;
            this.flushThread = new Thread(() -> {
                while (running.get()) {
                    try {
                        synchronized (flushLock) {
                            // 在锁内执行刷盘任务，避免并发问题
                            if (running.get()) { // 再次检查，防止关闭期间执行
                                flushTask.run();
                            }
                        }
                        Thread.sleep(flushIntervalMs); // 可配置的刷盘间隔
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
//                        logger.info("刷盘线程被中断");
                        break;
                    } catch (Exception e) {
                        // 改进错误处理，记录详细异常信息
                        logger.error("刷盘任务执行出错: " + e.getMessage(), e);
                        // 短暂休眠，避免在错误状态下频繁重试
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }
                // logger.info("刷盘线程退出");
            });
            this.flushThread.setDaemon(true);
            this.flushThread.setName("AOF-Flush-Thread");
        }

        public void start() {
            // logger.info("启动刷盘线程...");
            flushThread.start();
        }

        public void stop() {
//            logger.info("正在停止刷盘线程...");
            if (running.compareAndSet(true, false)) {
                synchronized (flushLock) {
                    // 在锁内通知停止，确保不会有新的刷盘操作启动
                    flushThread.interrupt();
                }
                try {
                    flushThread.join(3000);
                    if (flushThread.isAlive()) {
                        logger.warn("刷盘线程在3秒内未能正常停止");
                    } else {
//                        logger.info("刷盘线程已成功停止");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.warn("等待刷盘线程停止时被中断");
                }
            }
        }
    }

    /**
     * 生成测试命令
     */
    private static Resp generateCommand(Random random, boolean isSet) {
        // 根据配置的比例决定是否生成大数据命令
        boolean isLargeValue = isSet && random.nextDouble() < LARGE_VALUE_RATIO;
        return generateCommand(random, isSet, isLargeValue, isLargeValue ? LARGE_VALUE_SIZE : 0);
    }

    /**
     * 生成测试命令 - 支持大数据测试
     * @param random 随机数生成器
     * @param isSet 是否为SET命令
     * @param isLargeValue 是否生成大型值
     * @param dataSize 如果是大型值，指定数据大小（字节）
     * @return 生成的命令
     */
    private static Resp generateCommand(Random random, boolean isSet, boolean isLargeValue, int dataSize) {
        String key = "key:" + random.nextInt(10000);
        String value;

        if (isLargeValue) {
            // 生成大型值
            StringBuilder sb = new StringBuilder();
            // 使用10字节的字符串重复填充，以达到指定大小
            String filler = "largevalue";
            int iterations = dataSize / 10 + 1;
            for (int i = 0; i < iterations; i++) {
                sb.append(filler);
            }
            // 确保不超过指定大小
            if (sb.length() > dataSize) {
                value = sb.substring(0, dataSize);
            } else {
                value = sb.toString();
            }
            
            // 记录生成的大值大小
            logger.debug("生成大型值，大小: " + value.length() + " 字节");
        } else {
            // 原来的小型值
            value = "value:" + random.nextInt(1000000);
        }

        if (isSet) {
            // SET命令
            return new RespArray(new Resp[]{
                    new BulkString("SET"),
                    new BulkString(key),
                    new BulkString(value)
            });
        } else {
            // GET命令
            return new RespArray(new Resp[]{
                    new BulkString("GET"),
                    new BulkString(key)
            });
        }
    }

    /**
     * 运行JDK队列测试
     */
    private static TestResult runJdkQueueTest() throws Exception {
        return runJdkQueueTest(TEST_DURATION_SECONDS, THREAD_COUNT, 1000, false, 1024);
    }

    /**
     * 运行双缓冲队列测试
     */
    private static TestResult runDoubleBufferQueueTest() throws Exception {
        return runDoubleBufferQueueTest(TEST_DURATION_SECONDS, THREAD_COUNT, 1000, false, 1024);
    }

    /**
     * 运行JDK队列测试 - 支持自定义参数
     * @param testDurationSeconds 测试持续时间(秒)
     * @param threadCount 线程数
     * @param flushIntervalMs 刷盘间隔(毫秒)
     * @param useLargeData 是否使用大数据
     * @param dataSize 大数据大小(字节)
     */
    private static TestResult runJdkQueueTest(int testDurationSeconds, int threadCount,
                                            int flushIntervalMs, boolean useLargeData,
                                            int dataSize) throws Exception {
        logger.info("开始JDK原生阻塞队列测试...");
        logger.info(String.format("参数: 线程数=%d, 刷盘间隔=%dms, 大数据=%b, 数据大小=%d字节",
                threadCount, flushIntervalMs, useLargeData, dataSize));

        // 重置计数器
        totalBytesWritten.set(0);

        // 创建处理器和服务
        TestWriter writer = null;
        JdkQueueAOFProcessor processor = null;
        BackgroundFlushService flushService = null;
        ExecutorService executor = null;

        try {
            writer = new TestWriter(JDK_AOF_FILE);
            final JdkQueueAOFProcessor finalProcessor = new JdkQueueAOFProcessor(writer, BUFFER_SIZE, BATCH_SIZE);
            processor = finalProcessor; // 使用final变量来捕获lambda中的引用

            flushService = new BackgroundFlushService(() -> {
                try {
                    if (finalProcessor != null) {
                        finalProcessor.flush();
                    }
                } catch (IOException e) {
                    logger.error("JDK队列刷盘出错: " + e.getMessage(), e);
                }
            }, flushIntervalMs);

            // 重置计数器
            completedCommands.set(0);
            totalLatency.set(0);
            stopTest = false;

            // 启动刷盘服务
            flushService.start();

            // 创建工作线程
            executor = Executors.newFixedThreadPool(threadCount);
            final JdkQueueAOFProcessor lambdaProcessor = finalProcessor;

            for (int i = 0; i < threadCount; i++) {
                executor.submit(() -> {
                    Random random = new Random();
                    while (!stopTest) {
                        try {
                            // 随机生成SET或GET命令
                            boolean isSet = random.nextBoolean();
                            Resp command = generateCommand(random, isSet, useLargeData, dataSize);

                            // 测量延迟
                            long startTime = System.nanoTime();
                            lambdaProcessor.append(command);
                            long endTime = System.nanoTime();

                            // 更新统计信息
                            totalLatency.addAndGet(endTime - startTime);
                            completedCommands.incrementAndGet();
                        } catch (Exception e) {
                            logger.error("命令执行出错: " + e.getMessage(), e);
                        }
                    }
                });
            }

            // 等待测试时间
            Thread.sleep(testDurationSeconds * 1000L);
            stopTest = true;

            // 计算结果
            int totalCommands = completedCommands.get();
            double throughput = (double) totalCommands / testDurationSeconds;
            double avgLatency = totalCommands > 0 ? (double) totalLatency.get() / totalCommands / 1000000.0 : 0;
            
            // 使用append统计的字节数，与双缓冲队列保持一致
            long appendedBytes = finalProcessor.getAppendedBytes();
//            logger.info("JDK队列累计字节统计:");
//            logger.info("- 累计添加字节数: " + appendedBytes + " 字节");
//            logger.info("- writer记录字节数: " + writer.getBytesWritten() + " 字节");
            
            // 使用appendedBytes作为实际数据量，与双缓冲队列保持一致
            double bytesPerSecond = (double) appendedBytes / testDurationSeconds;

//            logger.info(String.format("JDK队列测试完成: 吞吐量 = %.2f 命令/秒, 平均延迟 = %.3f 毫秒, 数据吞吐量 = %.2f MB/秒",
//                    throughput, avgLatency, bytesPerSecond / (1024 * 1024)));

            return new TestResult(throughput, avgLatency, bytesPerSecond);
        } finally {
            // 确保资源正确关闭
            if (executor != null) {
                executor.shutdown();
                try {
                    if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                        logger.warn("工作线程池未能在5秒内关闭");
                        executor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.warn("等待线程池关闭时被中断");
                    executor.shutdownNow();
                }
            }

            if (flushService != null) {
                flushService.stop();
            }

            if (processor != null) {
                processor.stop();
            }

            if (writer != null) {
                writer.close();
            }
        }
    }

    /**
     * 运行双缓冲队列测试 - 支持自定义参数
     * @param testDurationSeconds 测试持续时间(秒)
     * @param threadCount 线程数
     * @param flushIntervalMs 刷盘间隔(毫秒)
     * @param useLargeData 是否使用大数据
     * @param dataSize 大数据大小(字节)
     */
    private static TestResult runDoubleBufferQueueTest(int testDurationSeconds, int threadCount,
                                                    int flushIntervalMs, boolean useLargeData,
                                                    int dataSize) throws Exception {
        logger.info("开始双缓冲队列测试...");
        logger.info(String.format("参数: 线程数=%d, 刷盘间隔=%dms, 大数据=%b, 数据大小=%d字节",
                threadCount, flushIntervalMs, useLargeData, dataSize));

        // 重置计数器
        totalBytesWritten.set(0);

        // 创建处理器和服务
        TestWriter writer = null;
        DoubleBufferAOFProcessor processor = null;
        BackgroundFlushService flushService = null;
        ExecutorService executor = null;

        try {
            writer = new TestWriter(DOUBLE_BUFFER_AOF_FILE);
            final DoubleBufferAOFProcessor finalProcessor = new DoubleBufferAOFProcessor(writer, BUFFER_SIZE, BATCH_SIZE);
            processor = finalProcessor;

            flushService = new BackgroundFlushService(() -> {
                try {
                    if (finalProcessor != null) {
                        finalProcessor.flush();
                    }
                } catch (IOException e) {
                    logger.error("双缓冲队列刷盘出错: " + e.getMessage(), e);
                }
            }, flushIntervalMs);

            // 重置计数器
            completedCommands.set(0);
            totalLatency.set(0);
            stopTest = false;

            // 启动刷盘服务
            flushService.start();

            // 创建工作线程
            executor = Executors.newFixedThreadPool(threadCount);
            final DoubleBufferAOFProcessor lambdaProcessor = finalProcessor;

            for (int i = 0; i < threadCount; i++) {
                executor.submit(() -> {
                    Random random = new Random();
                    while (!stopTest) {
                        try {
                            // 随机生成SET或GET命令
                            boolean isSet = random.nextBoolean();
                            Resp command = generateCommand(random, isSet, useLargeData, dataSize);

                            // 测量延迟
                            long startTime = System.nanoTime();
                            lambdaProcessor.append(command);
                            long endTime = System.nanoTime();

                            // 更新统计信息
                            totalLatency.addAndGet(endTime - startTime);
                            completedCommands.incrementAndGet();
                        } catch (Exception e) {
                            logger.error("命令执行出错: " + e.getMessage(), e);
                        }
                    }
                });
            }

            // 等待测试时间
            Thread.sleep(testDurationSeconds * 1000L);
            stopTest = true;
            
//            // 测试结束后强制执行一次刷盘，确保所有数据都被写入
//            logger.info("测试结束，强制执行最后一次刷盘");
            finalProcessor.flush();
            
            // 计算结果
            int totalCommands = completedCommands.get();
            double throughput = (double) totalCommands / testDurationSeconds;
            double avgLatency = totalCommands > 0 ? (double) totalLatency.get() / totalCommands / 1000000.0 : 0;
            
            // 使用双缓冲队列中的字节统计
            long flushedBytes = finalProcessor.bufferQueue.getTotalFlushedBytes();
            long appendedBytes = finalProcessor.getAppendedBytes();
//            logger.info("双缓冲队列累计字节统计:");
//            logger.info("- 累计添加字节数: " + appendedBytes + " 字节");
//            logger.info("- 累计刷盘字节数: " + flushedBytes + " 字节");
//            logger.info("- writer记录字节数: " + writer.getBytesWritten() + " 字节");

            // 使用appendedBytes作为实际数据量
            double bytesPerSecond = (double) appendedBytes / testDurationSeconds;

//            logger.info(String.format("双缓冲队列测试完成: 吞吐量 = %.2f 命令/秒, 平均延迟 = %.3f 毫秒, 数据吞吐量 = %.2f MB/秒",
//                    throughput, avgLatency, bytesPerSecond / (1024 * 1024)));

            return new TestResult(throughput, avgLatency, bytesPerSecond);
        } finally {
            // 确保资源正确关闭
            if (executor != null) {
                executor.shutdown();
                try {
                    if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                        logger.warn("工作线程池未能在5秒内关闭");
                        executor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.warn("等待线程池关闭时被中断");
                    executor.shutdownNow();
                }
            }

            if (flushService != null) {
                flushService.stop();
            }

            if (processor != null) {
                processor.stop();
            }

            if (writer != null) {
                writer.close();
            }
        }
    }

    /**
     * 高并发场景测试
     */
    private static void runHighConcurrencyTest() throws Exception {
        logger.info("=================== 高并发场景测试 ===================");
        logger.info("线程数: " + HIGH_CONCURRENCY_THREAD_COUNT);

        // JDK队列测试
        TestResult jdkResult = runJdkQueueTest(TEST_DURATION_SECONDS,
                                           HIGH_CONCURRENCY_THREAD_COUNT,
                                           1000, false, 1024);

        // 双缓冲队列测试
        TestResult doubleBufferResult = runDoubleBufferQueueTest(TEST_DURATION_SECONDS,
                                                             HIGH_CONCURRENCY_THREAD_COUNT,
                                                             1000, false, 1024);

        // 比较结果
        double throughputRatio = safeDivide(doubleBufferResult.throughput, jdkResult.throughput);
        double latencyRatio = safeDivide(jdkResult.latency, doubleBufferResult.latency);
        double bytesRatio = safeDivide(doubleBufferResult.bytesPerSecond, jdkResult.bytesPerSecond);

        logger.info("========== 高并发场景比较结果 ==========");
        logger.info(String.format("JDK队列: 吞吐量 = %.2f 命令/秒, 平均延迟 = %.3f 毫秒, 数据吞吐量 = %.2f MB/秒",
                jdkResult.throughput, jdkResult.latency, jdkResult.bytesPerSecond / (1024 * 1024)));
        logger.info(String.format("双缓冲队列: 吞吐量 = %.2f 命令/秒, 平均延迟 = %.3f 毫秒, 数据吞吐量 = %.2f MB/秒",
                doubleBufferResult.throughput, doubleBufferResult.latency, doubleBufferResult.bytesPerSecond / (1024 * 1024)));
        logger.info(String.format("性能比较: 双缓冲队列吞吐量是JDK队列的 %.2f 倍", throughputRatio));
        logger.info(String.format("延迟比较: 双缓冲队列延迟是JDK队列的 %.2f 倍 (值越小越好)", latencyRatio > 0 ? 1/latencyRatio : 0));
        logger.info(String.format("数据吞吐量比较: 双缓冲队列数据吞吐量是JDK队列的 %.2f 倍", bytesRatio));
    }

    /**
     * 大数据量测试
     */
    private static void runLargeDataTest() throws Exception {
        logger.info("=================== 大数据量测试 ===================");
        logger.info("数据大小: " + LARGE_DATA_SIZE + " 字节");

        // JDK队列测试
        TestResult jdkResult = runJdkQueueTest(TEST_DURATION_SECONDS,
                                           THREAD_COUNT,
                                           1000, true, LARGE_DATA_SIZE);

        // 双缓冲队列测试
        TestResult doubleBufferResult = runDoubleBufferQueueTest(TEST_DURATION_SECONDS,
                                                             THREAD_COUNT,
                                                             1000, true, LARGE_DATA_SIZE);

        // 比较结果
        double throughputRatio = safeDivide(doubleBufferResult.throughput, jdkResult.throughput);
        double latencyRatio = safeDivide(jdkResult.latency, doubleBufferResult.latency);
        double bytesRatio = safeDivide(doubleBufferResult.bytesPerSecond, jdkResult.bytesPerSecond);

        logger.info("========== 大数据量比较结果 ==========");
        logger.info(String.format("JDK队列: 吞吐量 = %.2f 命令/秒, 平均延迟 = %.3f 毫秒, 数据吞吐量 = %.2f MB/秒",
                jdkResult.throughput, jdkResult.latency, jdkResult.bytesPerSecond / (1024 * 1024)));
        logger.info(String.format("双缓冲队列: 吞吐量 = %.2f 命令/秒, 平均延迟 = %.3f 毫秒, 数据吞吐量 = %.2f MB/秒",
                doubleBufferResult.throughput, doubleBufferResult.latency, doubleBufferResult.bytesPerSecond / (1024 * 1024)));
        logger.info(String.format("性能比较: 双缓冲队列吞吐量是JDK队列的 %.2f 倍", throughputRatio));
        logger.info(String.format("延迟比较: 双缓冲队列延迟是JDK队列的 %.2f 倍 (值越小越好)", latencyRatio > 0 ? 1/latencyRatio : 0));
        logger.info(String.format("数据吞吐量比较: 双缓冲队列数据吞吐量是JDK队列的 %.2f 倍", bytesRatio));
    }

    /**
     * 高频刷盘测试
     */
    private static void runHighFlushRateTest() throws Exception {
        logger.info("=================== 高频刷盘测试 ===================");
        logger.info("刷盘间隔: " + HIGH_FLUSH_RATE_MS + " 毫秒");

        // JDK队列测试
        TestResult jdkResult = runJdkQueueTest(TEST_DURATION_SECONDS,
                                           THREAD_COUNT,
                                           HIGH_FLUSH_RATE_MS, false, 0);

        // 双缓冲队列测试
        TestResult doubleBufferResult = runDoubleBufferQueueTest(TEST_DURATION_SECONDS,
                                                             THREAD_COUNT,
                                                             HIGH_FLUSH_RATE_MS, false, 0);

        // 比较结果
        double throughputRatio = safeDivide(doubleBufferResult.throughput, jdkResult.throughput);
        double latencyRatio = safeDivide(jdkResult.latency, doubleBufferResult.latency);
        double bytesRatio = safeDivide(doubleBufferResult.bytesPerSecond, jdkResult.bytesPerSecond);

        logger.info("========== 高频刷盘比较结果 ==========");
        logger.info(String.format("JDK队列: 吞吐量 = %.2f 命令/秒, 平均延迟 = %.3f 毫秒, 数据吞吐量 = %.2f MB/秒",
                jdkResult.throughput, jdkResult.latency, jdkResult.bytesPerSecond / (1024 * 1024)));
        logger.info(String.format("双缓冲队列: 吞吐量 = %.2f 命令/秒, 平均延迟 = %.3f 毫秒, 数据吞吐量 = %.2f MB/秒",
                doubleBufferResult.throughput, doubleBufferResult.latency, doubleBufferResult.bytesPerSecond / (1024 * 1024)));
        logger.info(String.format("性能比较: 双缓冲队列吞吐量是JDK队列的 %.2f 倍", throughputRatio));
        logger.info(String.format("延迟比较: 双缓冲队列延迟是JDK队列的 %.2f 倍 (值越小越好)", latencyRatio > 0 ? 1/latencyRatio : 0));
        logger.info(String.format("数据吞吐量比较: 双缓冲队列数据吞吐量是JDK队列的 %.2f 倍", bytesRatio));
    }

    /**
     * 综合极端场景测试
     */
    private static void runExtremeTest() throws Exception {
        logger.info("=================== 综合极端场景测试 ===================");
        logger.info(String.format("线程数: %d, 刷盘间隔: %dms, 数据大小: %d字节",
                HIGH_CONCURRENCY_THREAD_COUNT, HIGH_FLUSH_RATE_MS, LARGE_DATA_SIZE));

        // JDK队列测试
        TestResult jdkResult = runJdkQueueTest(TEST_DURATION_SECONDS,
                                           HIGH_CONCURRENCY_THREAD_COUNT,
                                           HIGH_FLUSH_RATE_MS, true, LARGE_DATA_SIZE);

        // 双缓冲队列测试
        TestResult doubleBufferResult = runDoubleBufferQueueTest(TEST_DURATION_SECONDS,
                                                             HIGH_CONCURRENCY_THREAD_COUNT,
                                                             HIGH_FLUSH_RATE_MS, true, LARGE_DATA_SIZE);

        // 比较结果
        double throughputRatio = safeDivide(doubleBufferResult.throughput, jdkResult.throughput);
        double latencyRatio = safeDivide(jdkResult.latency, doubleBufferResult.latency);
        double bytesRatio = safeDivide(doubleBufferResult.bytesPerSecond, jdkResult.bytesPerSecond);

        logger.info("========== 综合极端场景比较结果 ==========");
        logger.info(String.format("JDK队列: 吞吐量 = %.2f 命令/秒, 平均延迟 = %.3f 毫秒, 数据吞吐量 = %.2f MB/秒",
                jdkResult.throughput, jdkResult.latency, jdkResult.bytesPerSecond / (1024 * 1024)));
        logger.info(String.format("双缓冲队列: 吞吐量 = %.2f 命令/秒, 平均延迟 = %.3f 毫秒, 数据吞吐量 = %.2f MB/秒",
                doubleBufferResult.throughput, doubleBufferResult.latency, doubleBufferResult.bytesPerSecond / (1024 * 1024)));
        logger.info(String.format("性能比较: 双缓冲队列吞吐量是JDK队列的 %.2f 倍", throughputRatio));
        logger.info(String.format("延迟比较: 双缓冲队列延迟是JDK队列的 %.2f 倍 ", latencyRatio > 0 ? 1/latencyRatio : 0));
        logger.info(String.format("数据吞吐量比较: 双缓冲队列数据吞吐量是JDK队列的 %.2f 倍", bytesRatio));
    }

    /**
     * 测试结果类
     */
    static class TestResult {
        final double throughput;      // 吞吐量 (命令/秒)
        final double latency;         // 平均延迟 (毫秒)
        final double bytesPerSecond;  // 数据吞吐量 (字节/秒)

        public TestResult(double throughput, double latency, double bytesPerSecond) {
            this.throughput = throughput;
            this.latency = latency;
            this.bytesPerSecond = bytesPerSecond;
        }
    }

    /**
     * 清理测试文件
     */
    private static void cleanupFiles() {
        File jdkFile = new File(JDK_AOF_FILE);
        if (jdkFile.exists()) {
            jdkFile.delete();
        }

        File doubleBufferFile = new File(DOUBLE_BUFFER_AOF_FILE);
        if (doubleBufferFile.exists()) {
            doubleBufferFile.delete();
        }
    }

    /**
     * 添加计算平均结果的方法
     */
    private static TestResult calculateAverageResult(List<TestResult> results) {
        if (results == null || results.isEmpty()) {
            // 如果没有结果，返回默认值
            return new TestResult(0, 0, 0);
        }
        
        double totalThroughput = 0;
        double totalLatency = 0;
        double totalBytesPerSecond = 0;

        for (TestResult result : results) {
            totalThroughput += result.throughput;
            totalLatency += result.latency;
            totalBytesPerSecond += result.bytesPerSecond;
        }

        return new TestResult(
                totalThroughput / results.size(),
                totalLatency / results.size(),
                totalBytesPerSecond / results.size()
        );
    }
    
    /**
     * 安全除法，防止除以零导致NaN
     */
    private static double safeDivide(double numerator, double denominator) {
        if (denominator == 0) {
            return 0; // 或者返回某个默认值
        }
        return numerator / denominator;
    }

    /**
     * 主测试方法
     */
    public static void main(String[] args) {
        try {
//            logger.info("开始AOF队列性能比较测试");
//            logger.info("配置: 线程数=" + THREAD_COUNT +
//                    ", 缓冲区大小=" + BUFFER_SIZE +
//                    ", 批处理大小=" + BATCH_SIZE +
//                    ", 测试时长=" + TEST_DURATION_SECONDS + "秒" +
//                    ", 测试迭代次数=" + TEST_ITERATIONS);

            // 清理测试文件
            cleanupFiles();

            // 预热阶段
            logger.info("开始预热阶段...");
            Thread.sleep(WARM_UP_SECONDS * 1000);



             // 运行标准测试
//             logger.info("=================== 标准场景测试 ===================");
             for (int i = 0; i < TEST_ITERATIONS; i++) {
//                 logger.info("开始第 " + (i+1) + "/" + TEST_ITERATIONS + " 轮测试");

                 // 运行JDK队列测试
                 TestResult jdkResult = runJdkQueueTest();
                 jdkResults.add(jdkResult);

                 // 运行双缓冲队列测试
                 TestResult doubleBufferResult = runDoubleBufferQueueTest();
                 doubleBufferResults.add(doubleBufferResult);

                 // 短暂休息，让系统恢复
                 Thread.sleep(2000);
             }

             // 计算平均结果
             TestResult avgJdkResult = calculateAverageResult(jdkResults);
             TestResult avgDoubleBufferResult = calculateAverageResult(doubleBufferResults);

             // 比较结果
             double throughputRatio = safeDivide(avgDoubleBufferResult.throughput, avgJdkResult.throughput);
             double latencyRatio = safeDivide(avgJdkResult.latency, avgDoubleBufferResult.latency);
             double bytesRatio = safeDivide(avgDoubleBufferResult.bytesPerSecond, avgJdkResult.bytesPerSecond);

             logger.info("========== 标准场景测试结果比较 (平均 " + TEST_ITERATIONS + " 次) ==========");
             logger.info(String.format("JDK队列: 吞吐量 = %.2f 命令/秒, 平均延迟 = %.3f 毫秒, 数据吞吐量 = %.2f MB/秒",
                     avgJdkResult.throughput, avgJdkResult.latency, avgJdkResult.bytesPerSecond / (1024 * 1024)));
             logger.info(String.format("双缓冲队列: 吞吐量 = %.2f 命令/秒, 平均延迟 = %.3f 毫秒, 数据吞吐量 = %.2f MB/秒",
                     avgDoubleBufferResult.throughput, avgDoubleBufferResult.latency, avgDoubleBufferResult.bytesPerSecond / (1024 * 1024)));
             logger.info(String.format("性能比较: 双缓冲队列吞吐量是JDK队列的 %.2f 倍", throughputRatio));
             logger.info(String.format("延迟比较: 双缓冲队列延迟是JDK队列的 %.2f 倍 (值越小越好)", latencyRatio > 0 ? 1/latencyRatio : 0));
             logger.info(String.format("数据吞吐量比较: 双缓冲队列数据吞吐量是JDK队列的 %.2f 倍", bytesRatio));


             // 运行极端场景测试
            runLargeDataTest();
             runHighConcurrencyTest();

             runHighFlushRateTest();
             runExtremeTest();

             // 清理测试文件
             cleanupFiles();

            logger.info("AOF队列性能比较测试完成");

        } catch (Exception e) {
            logger.error("测试执行出错", e);
        }
    }
}
