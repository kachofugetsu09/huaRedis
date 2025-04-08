package site.hnfy258.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutorGroup;
import org.apache.log4j.Logger;
import site.hnfy258.RedisCore;
import site.hnfy258.RedisCoreImpl;
import site.hnfy258.coder.MyCommandHandler;
import site.hnfy258.coder.MyDecoder;
import site.hnfy258.coder.MyResponseEncoder;
import site.hnfy258.channel.DefaultChannelSelectStrategy;
import site.hnfy258.channel.LocalChannelOption;
import site.hnfy258.aof.AOFHandler;

import java.io.IOException;

public class MyRedisService implements RedisService {
    private static final Logger logger = Logger.getLogger(MyRedisService.class);

    // 通过修改此标志来开启或关闭AOF功能
    private static final boolean ENABLE_AOF = true;

    private final int port;
    private final RedisCore redisCore;
    private final LocalChannelOption channelOption;
    private final EventExecutorGroup commandExecutor;
    private final AOFHandler aofHandler;

    private Channel serverChannel;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    public MyRedisService(int port) {
        this.port = port;
        this.redisCore = new RedisCoreImpl();
        this.channelOption = new DefaultChannelSelectStrategy().select();
        this.commandExecutor = new DefaultEventExecutorGroup(1,
                new DefaultThreadFactory("redis-cmd"));

        // 根据配置决定是否初始化AOF处理器
        if (ENABLE_AOF) {
            this.aofHandler = new AOFHandler("redis.aof");
            this.aofHandler.setSyncStrategy(AOFHandler.AOFSyncStrategy.EVERYSEC);
        } else {
            this.aofHandler = null;
            //logger.info("AOF功能已禁用");
        }
    }

    @Override
    public void start() {
        this.bossGroup = channelOption.boss();
        this.workerGroup = channelOption.selectors();

        try {
            // 只有在启用AOF时才启动AOF处理器
            if (ENABLE_AOF) {
                this.aofHandler.start();
                this.aofHandler.load(redisCore);
                //logger.info("AOF持久化已启用");
            }

            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(channelOption.getChannelClass())
                    .option(ChannelOption.SO_BACKLOG, 1024)
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.SO_RCVBUF, 32 * 1024)
                    .childOption(ChannelOption.SO_SNDBUF, 32 * 1024)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast(new MyDecoder());
                            pipeline.addLast(new MyResponseEncoder());
                            pipeline.addLast(commandExecutor, new MyCommandHandler(redisCore, aofHandler));
                        }
                    });

            ChannelFuture future = bootstrap.bind(port).sync();
            this.serverChannel = future.channel();
            //logger.info("Redis服务已启动，监听端口: " + port);

            future.channel().closeFuture().addListener((ChannelFutureListener) f -> {
                if (f.isSuccess()) {
                    //logger.info("服务器正常关闭");
                } else {
                    logger.error("服务器异常关闭: " + f.cause());
                }
            }).sync();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("服务器启动被中断", e);
        } catch (Exception e) {
            logger.error("服务器启动异常", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        //logger.info("开始关闭Redis服务...");

        // 首先关闭AOF处理器，确保所有命令都已写入
        if (ENABLE_AOF && aofHandler != null) {
            try {
                //logger.info("正在关闭AOF处理器...");
                aofHandler.stop();
                //logger.info("AOF处理器已关闭");
            } catch (Exception e) {
                logger.error("关闭AOF处理器时出错", e);
            }
        }

        // 然后关闭网络资源
        if (serverChannel != null) {
            //logger.info("正在关闭服务器通道...");
            serverChannel.close().syncUninterruptibly();
            //logger.info("服务器通道已关闭");
        }

        if (commandExecutor != null) {
            //logger.info("正在关闭命令执行器...");
            commandExecutor.shutdownGracefully().syncUninterruptibly();
            //logger.info("命令执行器已关闭");
        }

        if (workerGroup != null) {
            //logger.info("正在关闭工作线程组...");
            workerGroup.shutdownGracefully().syncUninterruptibly();
            //logger.info("工作线程组已关闭");
        }

        if (bossGroup != null) {
            //logger.info("正在关闭主线程组...");
            bossGroup.shutdownGracefully().syncUninterruptibly();
            //logger.info("主线程组已关闭");
        }

        //logger.info("Redis服务已完全关闭");
    }

    @Override
    public MyRedisService getRedisService() {
        return this;
    }

    @Override
    public RedisCore getRedisCore() {
        return redisCore;
    }

}