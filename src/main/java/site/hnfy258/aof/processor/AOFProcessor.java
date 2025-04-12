package site.hnfy258.aof.processor;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.log4j.Logger;
import site.hnfy258.aof.writer.Writer;
import site.hnfy258.protocal.Resp;
import site.hnfy258.utils.DoubleBufferBlockingQueue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * AOF处理器，负责将命令追加到AOF文件
 */
public class AOFProcessor implements Processor {
    private static final Logger logger = Logger.getLogger(AOFProcessor.class);

    private final DoubleBufferBlockingQueue bufferQueue;    // 双缓冲队列
    private final LinkedBlockingQueue<Resp> commandQueue;   // 命令队列
    private final Writer writer;                            // 文件写入器
    private final AtomicBoolean running;                    // 运行状态标志

    /**
     * 构造AOF处理器
     * @param writer 文件写入器
     * @param bufferSize 缓冲区大小
     */
    public AOFProcessor(Writer writer, int bufferSize) {
        // 1. 初始化组件
        this.writer = writer;
        this.commandQueue = new LinkedBlockingQueue<>();
        this.bufferQueue = new DoubleBufferBlockingQueue(bufferSize);
        this.running = new AtomicBoolean(true);
    }

    @Override
    public void append(Resp command) {
        // 1. 将命令添加到命令队列
        if (running.get()) {
            commandQueue.offer(command);
        }
    }

    @Override
    public void processCommand() throws InterruptedException, IOException {
        // 1. 从命令队列中获取命令，最多等待100毫秒
        Resp command = commandQueue.poll(100, TimeUnit.MILLISECONDS);
        if (command != null) {
            // 2. 将命令序列化到缓冲区
            ByteBuf buf = Unpooled.buffer();
            command.write(command, buf);
            ByteBuffer byteBuffer = buf.nioBuffer();

            // 3. 将序列化后的缓冲区加入到双缓冲队列
            bufferQueue.put(byteBuffer);
        }
    }

    @Override
    public void flush() throws IOException {
        // 1. 从双缓冲队列中获取待写入的缓冲区
        ByteBuffer buffer = bufferQueue.poll();

        // 2. 将缓冲区内容写入到AOF文件
        writer.write(buffer);
    }

    @Override
    public void stop() {
        // 1. 停止处理器
        running.set(false);
    }

    @Override
    public boolean isRunning() {
        // 1. 返回处理器当前运行状态
        return running.get();
    }
}
