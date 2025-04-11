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

public class AOFHandler {
    private static final Logger logger = Logger.getLogger(AOFHandler.class);

    private final String filename;
    private final Writer writer;
    private final Processor processor;
    private final Loader loader;
    private final AOFBackgroundService backgroundService;
    private AOFSyncStrategy syncStrategy;

    public AOFHandler(String filename) throws IOException {
        this.filename = filename;
        this.syncStrategy = AOFSyncStrategy.EVERYSEC;
        this.writer = new AOFWriter(filename, syncStrategy);
        this.processor = new AOFProcessor(writer, 2 * 1024 * 1024);
        this.loader = new AOFLoader();
        this.backgroundService = new AOFBackgroundService(processor, syncStrategy);
    }

    public void start() {
        backgroundService.start();
    }

    public void append(Resp command) {
        processor.append(command);
    }

    public void stop() {
        backgroundService.stop();
        writer.close();
    }

    public void setSyncStrategy(AOFSyncStrategy strategy) {
        this.syncStrategy = strategy;
    }

    public void load(RedisCore redisCore) throws IOException {
        loader.load(filename, redisCore);
    }

}