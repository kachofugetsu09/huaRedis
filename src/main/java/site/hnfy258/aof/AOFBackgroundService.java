package site.hnfy258.aof;

import org.apache.log4j.Logger;
import site.hnfy258.aof.processor.Processor;

import java.io.IOException;

/**
 * AOF后台服务，负责管理AOF的后台保存和同步操作
 */
public class AOFBackgroundService {
    private static final Logger logger = Logger.getLogger(AOFBackgroundService.class);

    private final Processor processor;           // AOF处理器
    private final AOFSyncStrategy syncStrategy;  // 同步策略
    private Thread bgSaveThread;                 // 后台保存线程
    private Thread syncThread;                   // 同步线程

    /**
     * 构造AOF后台服务
     * @param processor AOF处理器
     * @param syncStrategy 同步策略
     */
    public AOFBackgroundService(Processor processor, AOFSyncStrategy syncStrategy) {
        this.processor = processor;
        this.syncStrategy = syncStrategy;
    }

    /**
     * 启动AOF后台服务
     */
    public void start() {
        // 1. 创建并启动后台保存线程
        this.bgSaveThread = new Thread(this::backgroundSave);
        this.bgSaveThread.setName("aof-background-save");
        this.bgSaveThread.setDaemon(true);
        this.bgSaveThread.start();

        // 2. 如果同步策略为每秒同步，创建并启动同步线程
        if (syncStrategy == AOFSyncStrategy.EVERYSEC) {
            this.syncThread = new Thread(this::backgroundSync);
            this.syncThread.setName("aof-background-sync");
            this.syncThread.setDaemon(true);
            this.syncThread.start();
        }
    }

    /**
     * 后台保存操作
     */
    private void backgroundSave() {
        while (processor.isRunning()) {
            try {
                // 1. 处理命令
                processor.processCommand();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("AOF写入错误", e);
                if (processor.isRunning()) {
                    // 2. 如果发生错误，停止AOF功能
                    logger.error("由于错误禁用AOF功能");
                    processor.stop();
                }
            }
        }

        try {
            // 3. 最终刷盘操作
            processor.flush();
        } catch (IOException e) {
            logger.error("最终AOF刷盘错误", e);
        }
    }

    /**
     * 后台同步操作，每隔一秒进行一次刷盘
     */
    private void backgroundSync() {
        while (processor.isRunning()) {
            try {
                // 1. 等待1秒
                Thread.sleep(1000);
                // 2. 执行刷盘操作
                processor.flush();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (IOException e) {
                logger.error("AOF同步错误", e);
            }
        }
    }

    /**
     * 停止AOF后台服务
     */
    public void stop() {
        // 1. 停止处理器
        processor.stop();

        // 2. 停止后台保存线程
        if (bgSaveThread != null && bgSaveThread.isAlive()) {
            bgSaveThread.interrupt();
            try {
                bgSaveThread.join(3000);
                if (bgSaveThread.isAlive()) {
                    logger.warn("AOF background thread did not terminate in time");
                }
            } catch (InterruptedException e) {
                logger.warn("Interrupted while waiting for AOF background thread to stop");
                Thread.currentThread().interrupt();
            }
        }
    }
}