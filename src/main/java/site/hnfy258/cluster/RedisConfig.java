package site.hnfy258.cluster;

public class RedisConfig {
    private static final boolean ENABLE_SHARDING = false; // 可以从配置文件中读取

    public static boolean isShardingEnabled() {
        return ENABLE_SHARDING;
    }
}
