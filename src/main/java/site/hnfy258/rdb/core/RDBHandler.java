package site.hnfy258.rdb.core;

import site.hnfy258.RedisCore;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.rdb.executor.ExecutorManager;
import site.hnfy258.rdb.policy.ChangeTracker;
import site.hnfy258.rdb.policy.SavePolicy;

import java.io.IOException;
import java.util.concurrent.*;

public class RDBHandler {
    private static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(RDBHandler.class);

    private final RedisCore redisCore;
    private final RDBSaver saver;
    private final RDBLoader loader;
    private final SavePolicy savePolicy;
    private final ChangeTracker changeTracker;
    private final ExecutorManager executorManager;
    private final RDBFileManager fileManager;

    private volatile boolean isSaving = false;

    public RDBHandler(RedisCore redisCore) {
        this.redisCore = redisCore;
        this.executorManager = new ExecutorManager();
        this.saver = new RDBSaver(redisCore, executorManager.getIoExecutor());
        this.loader = new RDBLoader(redisCore);
        this.savePolicy = new SavePolicy();
        this.changeTracker = new ChangeTracker();
        this.fileManager = new RDBFileManager();
    }

    public void initialize() {
        try {
            load();
            startAutoSave();
        } catch (IOException e) {
            logger.error("初始化RDB处理器失败", e);
        }
    }

    private void startAutoSave() {
        executorManager.getScheduler().scheduleAtFixedRate(() -> {
            try {
                checkSaveConditions();
            } catch (Exception e) {
                logger.error("自动保存检查失败", e);
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    private void checkSaveConditions() {
        if (isSaving) return;

        long now = System.currentTimeMillis() / 1000;
        if (savePolicy.shouldFullSave(now)) {
            bgsave(true);
        } else if (changeTracker.hasModifications()) {
            bgsave(false);
        }
    }

    public void notifyDataChanged(int dbIndex, BytesWrapper key) {
        savePolicy.recordChange();
        changeTracker.trackChange(dbIndex, key);
    }

    public CompletableFuture<Boolean> save() {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        if (!bgsave(true)) {
            future.completeExceptionally(new IllegalStateException("已有保存任务运行"));
        } else {
            executorManager.getSaveExecutor().execute(() -> {
                while (isSaving) Thread.yield();
                future.complete(true);
            });
        }
        return future;
    }

    public boolean bgsave(boolean fullSave) {
        if (isSaving) {
            logger.warn("已有RDB保存任务在进行中，忽略此次请求");
            return false;
        }

        isSaving = true;
        CompletableFuture<Void> saveFuture;

        if (fullSave) {
            logger.info("开始后台全量保存RDB文件");
            saveFuture = saver.saveFullRDB();
        } else {
            logger.info("开始后台增量保存RDB文件");
            saveFuture = saver.saveIncrementalRDB(changeTracker.getModifiedData());
        }

        saveFuture.whenComplete((result, ex) -> {
            if (ex != null) {
                logger.error("后台保存RDB文件失败", ex);
            } else {
                logger.info("RDB文件" + (fullSave ? "全量" : "增量") + "后台保存完成");
                if (fullSave) {
                    changeTracker.clear();
                }
            }
            isSaving = false;
        });

        return true;
    }

    public void load() throws IOException {
        logger.info("开始加载RDB文件");
        loader.clearAllDatabases();

        if (fileManager.fullRdbExists()) {
            loader.loadRDB(fileManager.getFullRdbFile());
        }

        if (fileManager.incrementalRdbExists()) {
            loader.loadRDB(fileManager.getIncrementalRdbFile());
        }

        logger.info("RDB文件加载成功");
    }

    public void shutdown() {
        executorManager.shutdown();
    }

    public boolean isSaving() {
        return isSaving;
    }
}