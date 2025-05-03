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

        // 2. 如果同步策略为每秒同步，创建并启动同步线程
        if (syncStrategy == AOFSyncStrategy.EVERYSEC) {
            this.syncThread = new Thread(this::backgroundSync);
            this.syncThread.setName("aof-background-sync");
            this.syncThread.setDaemon(true);
            this.syncThread.start();
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

        //2. 停止同步线程
        if(syncThread != null && syncThread.isAlive() && !syncThread.isInterrupted()){
            syncThread.interrupt();
            try{
                syncThread.join(3000);
                logger.info("AOFBackgroundService.stop(): 同步线程已停止");
            }catch(InterruptedException e){
                logger.error("AOFBackgroundService.stop(): 等待同步线程终止时被中断", e);
                Thread.currentThread().interrupt();
            }
        }

        //3.确保最后一次刷盘
        try{
            processor.flush();
            logger.info("AOFBackgroundService.stop(): 最终刷盘完成");
        }catch(IOException e){
            logger.error("AOFBackgroundService.stop(): 最终刷盘失败", e);
        }
    }
}