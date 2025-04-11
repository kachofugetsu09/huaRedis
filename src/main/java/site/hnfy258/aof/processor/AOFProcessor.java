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

public class AOFProcessor implements Processor {
    private static final Logger logger = Logger.getLogger(AOFProcessor.class);
    
    private final DoubleBufferBlockingQueue bufferQueue;
    private final LinkedBlockingQueue<Resp> commandQueue;
    private final Writer writer;
    private final AtomicBoolean running;
    
    public AOFProcessor(Writer writer, int bufferSize) {
        this.writer = writer;
        this.commandQueue = new LinkedBlockingQueue<>();
        this.bufferQueue = new DoubleBufferBlockingQueue(bufferSize);
        this.running = new AtomicBoolean(true);
    }
    
    @Override
    public void append(Resp command) {
        if (running.get()) {
            commandQueue.offer(command);
        }
    }
    
    @Override
    public void processCommand() throws InterruptedException, IOException {
        Resp command = commandQueue.poll(100, TimeUnit.MILLISECONDS);
        if (command != null) {
            ByteBuf buf = Unpooled.buffer();
            command.write(command, buf);
            ByteBuffer byteBuffer = buf.nioBuffer();
            bufferQueue.put(byteBuffer);
        }
    }
    
    @Override
    public void flush() throws IOException {
        ByteBuffer buffer = bufferQueue.poll();
        writer.write(buffer);
    }
    
    @Override
    public void stop() {
        running.set(false);
    }
    
    @Override
    public boolean isRunning() {
        return running.get();
    }
}