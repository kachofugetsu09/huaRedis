package site.hnfy258.aof.loader;

import site.hnfy258.RedisCore;

import java.io.IOException;

public interface Loader {
    void load(String filename, RedisCore redisCore) throws IOException;

    class LoadStats {
        int commandsLoaded = 0;
        int commandsFailed = 0;
    }
}
