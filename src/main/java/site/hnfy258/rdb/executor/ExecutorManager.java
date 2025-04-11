package site.hnfy258.rdb.executor;

import java.util.concurrent.*;

public class ExecutorManager implements Executor {
    private final ScheduledExecutorService scheduler;
    private final ExecutorService saveExecutor;
    private final ExecutorService ioExecutor;

    public ExecutorManager() {
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "rdb-scheduler");
            t.setDaemon(true);
            return t;
        });
        this.saveExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "rdb-saver");
            t.setDaemon(true);
            return t;
        });
        this.ioExecutor = Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors(),
            r -> {
                Thread t = new Thread(r, "rdb-io-worker");
                t.setDaemon(true);
                return t;
            }
        );
    }

    public ScheduledExecutorService getScheduler() {
        return scheduler;
    }

    public ExecutorService getSaveExecutor() {
        return saveExecutor;
    }

    public ExecutorService getIoExecutor() {
        return ioExecutor;
    }

    public void shutdown() {
        scheduler.shutdown();
        saveExecutor.shutdown();
        ioExecutor.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
            if (!saveExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                saveExecutor.shutdownNow();
            }
            if (!ioExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                ioExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}