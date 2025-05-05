package site.hnfy258.aof;

import org.apache.log4j.Logger;
import site.hnfy258.aof.processor.Processor;

import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * AOF后台服务，负责管理AOF的后台保存和同步操作
 */
public class AOFBackgroundService {
    private static final Logger logger = Logger.getLogger(AOFBackgroundService.class);

    private final Processor processor;           // AOF处理器
    private final AOFSyncStrategy syncStrategy;  // 同步策略
    
    // 使用ScheduledExecutorService代替手动线程管理
    private final ScheduledExecutorService scheduler;
    private ScheduledFuture<?> syncTask;
    
    // 线程工厂，为线程命名
    private static class AOFThreadFactory implements ThreadFactory {
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        
        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r, "aof-background-sync-" + threadNumber.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        }
    }

    /**
     * 构造AOF后台服务
     * @param processor AOF处理器
     * @param syncStrategy 同步策略
     */
    public AOFBackgroundService(Processor processor, AOFSyncStrategy syncStrategy) {
        this.processor = processor;
        this.syncStrategy = syncStrategy;
        // 创建单线程的调度器
        this.scheduler = Executors.newSingleThreadScheduledExecutor(new AOFThreadFactory());
    }

    /**
     * 启动AOF后台服务
     */
    public void start() {
        // 如果同步策略为每秒同步，创建并启动调度任务
        if (syncStrategy == AOFSyncStrategy.EVERYSEC) {
            syncTask = scheduler.scheduleWithFixedDelay(
                this::performSync,    // 执行同步的方法
                1,                    // 初始延迟1秒
                1,                    // 每1秒执行一次
                TimeUnit.SECONDS      // 时间单位
            );
            logger.info("AOF后台同步服务已启动，同步策略: " + syncStrategy);
        } else if (syncStrategy == AOFSyncStrategy.ALWAYS) {
            logger.info("AOF同步策略为ALWAYS，不启动后台同步");
        } else {
            logger.info("AOF同步策略为NO，不进行同步");
        }
    }

    /**
     * 执行同步操作，作为调度任务的目标方法
     */
    private void performSync() {
        try {
            if (processor.isRunning()) {
                processor.flush();
                
                // 每10次同步打印一次日志
                if (logger.isDebugEnabled() && System.currentTimeMillis() % 10000 < 1000) {
                    logger.debug("AOF后台同步执行成功");
                }
            }
        } catch (IOException e) {
            logger.error("AOF同步错误", e);
        } catch (Exception e) {
            logger.error("AOF同步过程中发生未预期的错误", e);
        }
    }

    /**
     * 停止AOF后台服务
     */
    public void stop() {
        logger.info("正在停止AOF后台服务...");
        
        // 1. 如果有同步任务在运行，停止它
        if (syncTask != null && !syncTask.isDone()) {
            syncTask.cancel(false); // 尝试优雅停止，不中断正在执行的任务
            logger.info("AOF同步任务已取消");
        }
        
        // 2. 停止调度器
        if (scheduler != null && !scheduler.isShutdown()) {
            try {
                // 首先尝试优雅关闭
                scheduler.shutdown();
                
                // 等待终止，最多5秒
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    // 如果超时，强制关闭
                    scheduler.shutdownNow();
                    logger.warn("AOF调度器未能在5秒内优雅关闭，已强制终止");
                } else {
                    logger.info("AOF调度器已优雅关闭");
                }
            } catch (InterruptedException e) {
                // 如果当前线程被中断，强制关闭并恢复中断状态
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
                logger.warn("等待AOF调度器关闭时被中断，已强制终止");
            }
        }
        
        // 3. 停止处理器
        processor.stop();

        // 4. 确保最后一次刷盘
        try {
            processor.flush();
            logger.info("AOF最终刷盘完成，后台服务已停止");
        } catch (IOException e) {
            logger.error("AOF最终刷盘失败", e);
        }
    }
}