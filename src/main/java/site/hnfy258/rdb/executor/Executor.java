package site.hnfy258.rdb.executor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public interface Executor {
    ScheduledExecutorService getScheduler();
    ExecutorService getSaveExecutor();
    ExecutorService getIoExecutor();
    void shutdown();
}
