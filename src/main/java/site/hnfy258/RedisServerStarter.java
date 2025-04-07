package site.hnfy258;

import org.apache.log4j.Logger;
import site.hnfy258.server.MyRedisService;
import site.hnfy258.server.RedisService;

public class RedisServerStarter {
    public static void main(String[] args) {
        Logger logger = Logger.getLogger(RedisServerStarter.class);
        //默认端口
        int port = 6379;

        if (args.length > 0) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                logger.error("Invalid port number, using default: " + port);
            }
        }

        // 创建并启动Redis服务
        RedisService redisService = new MyRedisService(port);

        // 添加关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down Redis server...");
            redisService.close();
        }));

        redisService.start();
    }
}