package site.hnfy258.aof;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.CompleteFuture;
import org.apache.log4j.Logger;
import site.hnfy258.RedisCore;
import site.hnfy258.aof.loader.AOFLoader;
import site.hnfy258.aof.loader.Loader;
import site.hnfy258.aof.processor.AOFProcessor;
import site.hnfy258.aof.processor.Processor;
import site.hnfy258.aof.rewriter.AOFRewriter;
import site.hnfy258.aof.writer.AOFWriter;
import site.hnfy258.aof.writer.Writer;
import site.hnfy258.protocal.Resp;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * AOF处理器，负责管理AOF的各个组件和操作
 */
public class AOFHandler {
    private static final Logger logger = Logger.getLogger(AOFHandler.class);

    private final String filename;               // AOF文件名
    private final Writer writer;                 // AOF写入器
    private final Processor processor;           // AOF处理器
    private final Loader loader;                 // AOF加载器
    private final AOFBackgroundService backgroundService;  // AOF后台服务
    private AOFSyncStrategy syncStrategy;        // 同步策略

    private AOFRewriter rewriter;
    private final AtomicBoolean rewriting;
    private Thread rewriteThread;
    List<ByteBuffer> rewriteBuffer;
    private AtomicBoolean collectingRewriteBuffer = new AtomicBoolean(false);

    private CompletableFuture<Boolean> rewriteFuture;

    /**
     * 构造AOF处理器
     * @param filename AOF文件名
     * @throws IOException 如果创建文件失败
     */
    public AOFHandler(String filename, RedisCore redisCore) throws IOException {
        this.filename = filename;
        // 1. 设置默认同步策略为每秒同步
        this.syncStrategy = AOFSyncStrategy.EVERYSEC;
        // 2. 创建AOF写入器
        this.writer = new AOFWriter(filename, syncStrategy);
        // 3. 创建AOF处理器，设置缓冲区大小为2MB
        this.processor = new AOFProcessor(writer, 2 * 1024 * 1024);
        // 4. 创建AOF加载器
        this.loader = new AOFLoader();
        // 5. 创建AOF后台服务
        this.backgroundService = new AOFBackgroundService(processor, syncStrategy);

        this.rewriter = new AOFRewriter(redisCore, filename,2*1024*1024);
        this.rewriting = new AtomicBoolean(false);
    }

    /**
     * 启动AOF处理器
     */
    public void start() {
        // 启动后台服务
        backgroundService.start();
    }

    /**
     * 追加命令到AOF
     * @param command 要追加的命令
     */
    public void append(Resp command) {
        // 将命令交给处理器，处理器会从对应的Command队列中获取命令
        processor.append(command);

        if(collectingRewriteBuffer.get() && rewriteBuffer != null){
            ByteBuf buf = Unpooled.directBuffer();
            command.write(command, buf);
            ByteBuffer byteBuffer = buf.nioBuffer();
            rewriteBuffer.add(byteBuffer);
            buf.release();
        }
    }

    /**
     * 停止AOF处理器
     */
    public void stop() {
        // 1. 停止后台服务
        backgroundService.stop();
        // 2. 关闭写入器
        writer.close();

    }

    /**
     * 设置同步策略
     * @param strategy 新的同步策略
     */
    public void setSyncStrategy(AOFSyncStrategy strategy) {
        this.syncStrategy = strategy;
    }

    /**
     * 加载AOF文件
     * @param redisCore Redis核心实例
     * @throws IOException 如果加载失败
     */
    public void load(RedisCore redisCore) throws IOException {
        // 使用加载器加载AOF文件
        loader.load(filename, redisCore);
    }

    public CompletableFuture<Boolean> startRewrite() {
        if (rewriting.get()) {
            logger.warn("已有重写任务在进行中，忽略此次请求");
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            future.complete(false);
            return future;
        }
        if (!rewriter.canRewrite()) {
            logger.warn("重写文件失败，请检查重写文件是否正在被使用");
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            future.complete(false);
            return future;
        }

        if (rewriting.compareAndSet(false, true)) {
            rewriteFuture = new CompletableFuture<>();
            CompletableFuture.runAsync(() -> {
                try {
                    // 执行重写逻辑前，确保所有数据都已刷盘
                    processor.flush();

                    // 执行重写
                    boolean success = rewriter.rewrite();

                    if (success) {
                        logger.info("AOF重写成功完成");
                    } else {
                        logger.warn("AOF重写失败");
                    }

                    rewriteFuture.complete(success);
                } catch (Exception e) {
                    logger.error("AOF重写过程中出错", e);
                    rewriteFuture.completeExceptionally(e);
                } finally {
                    rewriting.set(false);
                }
            });
        }
        return rewriteFuture;
        }



    public boolean isRewriting(){
        return rewriting.get();
    }



    public void startRewriteBuffer() {
        rewriteBuffer = Collections.synchronizedList(new ArrayList<>());
        collectingRewriteBuffer.set(true);
    }


    public void discardRewriteBuffer() {
        collectingRewriteBuffer.set(false);
        rewriteBuffer = null;
    }

    public List<ByteBuffer> stopRewriteBufferAndGet() {
        collectingRewriteBuffer.set(false);
        List<ByteBuffer> result = rewriteBuffer;
        rewriteBuffer = null;
        return result;
    }
}
