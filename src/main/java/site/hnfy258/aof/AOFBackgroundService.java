package site.hnfy258.aof;

import org.apache.log4j.Logger;
import site.hnfy258.aof.processor.Processor;

import java.io.IOException;

public class AOFBackgroundService {
    private static final Logger logger = Logger.getLogger(AOFBackgroundService.class);
    
    private final Processor processor;
    private final AOFSyncStrategy syncStrategy;
    private Thread bgSaveThread;
    private Thread syncThread;
    
    public AOFBackgroundService(Processor processor, AOFSyncStrategy syncStrategy) {
        this.processor = processor;
        this.syncStrategy = syncStrategy;
    }
    
    public void start() {
        this.bgSaveThread = new Thread(this::backgroundSave);
        this.bgSaveThread.setName("aof-background-save");
        this.bgSaveThread.setDaemon(true);
        this.bgSaveThread.start();
        
        if (syncStrategy == AOFSyncStrategy.EVERYSEC) {
            this.syncThread = new Thread(this::backgroundSync);
            this.syncThread.setName("aof-background-sync");
            this.syncThread.setDaemon(true);
            this.syncThread.start();
        }
    }
    
    private void backgroundSave() {
        while (processor.isRunning()) {
            try {
                processor.processCommand();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("AOF写入错误", e);
                if (processor.isRunning()) {
                    logger.error("由于错误禁用AOF功能");
                    processor.stop();
                }
            }
        }
        
        try {
            processor.flush();
        } catch (IOException e) {
            logger.error("最终AOF刷盘错误", e);
        }
    }
    
    private void backgroundSync() {
        while (processor.isRunning()) {
            try {
                Thread.sleep(1000);
                processor.flush();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (IOException e) {
                logger.error("AOF同步错误", e);
            }
        }
    }
    
    public void stop() {
        processor.stop();
        
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