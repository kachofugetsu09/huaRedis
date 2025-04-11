package site.hnfy258;

import org.apache.log4j.Logger;
import site.hnfy258.server.MyRedisService;
import site.hnfy258.server.RedisService;

import java.io.IOException;

public class RedisServerStarter {
    private static final Logger logger = Logger.getLogger(RedisServerStarter.class);
    private static RedisService redisService;

    public static void main(String[] args) throws IOException {
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
        redisService = new MyRedisService(port);

        // 添加关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            //logger.info("正在关闭Redis服务器...");
            try {
                redisService.close();
                // 给一些时间让日志完成写入
                Thread.sleep(500);
            } catch (Exception e) {
                logger.error("关闭服务时发生错误", e);
            }
            //logger.info("Redis服务器已关闭");
        }));

        redisService.start();
    }
}
