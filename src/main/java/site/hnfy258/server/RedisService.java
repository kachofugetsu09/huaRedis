package site.hnfy258.server;

import site.hnfy258.RedisCore;

public interface RedisService {
    void start();
    void close();
    MyRedisService getRedisService();

    RedisCore getRedisCore();
}
