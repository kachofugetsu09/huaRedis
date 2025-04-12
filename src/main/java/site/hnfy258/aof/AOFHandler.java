package site.hnfy258.aof;

import org.apache.log4j.Logger;
import site.hnfy258.RedisCore;
import site.hnfy258.aof.loader.AOFLoader;
import site.hnfy258.aof.loader.Loader;
import site.hnfy258.aof.processor.AOFProcessor;
import site.hnfy258.aof.processor.Processor;
import site.hnfy258.aof.writer.AOFWriter;
import site.hnfy258.aof.writer.Writer;
import site.hnfy258.protocal.Resp;

import java.io.IOException;

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

    /**
     * 构造AOF处理器
     * @param filename AOF文件名
     * @throws IOException 如果创建文件失败
     */
    public AOFHandler(String filename) throws IOException {
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
}
