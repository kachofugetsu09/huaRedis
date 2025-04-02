package site.hnfy258.server;

public interface RedisService {
    void start();
    void close();
    MyRedisService getRedisService();
}
