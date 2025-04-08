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

public class MyRedisService implements RedisService {
    private static final Logger logger = Logger.getLogger(MyRedisService.class);

    private final int port;
    private final RedisCore redisCore;
    private final LocalChannelOption channelOption;
    private final EventExecutorGroup commandExecutor;

    private Channel serverChannel;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    public MyRedisService(int port) {
        this.port = port;
        this.redisCore = new RedisCoreImpl();
        this.channelOption = new DefaultChannelSelectStrategy().select();
        this.commandExecutor = new DefaultEventExecutorGroup(1,
                new DefaultThreadFactory("redis-cmd"));
    }

    @Override
    public void start() {
        this.bossGroup = channelOption.boss();
        this.workerGroup = channelOption.selectors();

        try {
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
                            // I/O线程处理编解码
                            pipeline.addLast(new MyDecoder());
                            pipeline.addLast(new MyResponseEncoder());
                            // 命令处理切换到专用线程
                            pipeline.addLast(commandExecutor, new MyCommandHandler(redisCore));
                        }
                    });

            ChannelFuture future = bootstrap.bind(port).sync();
            this.serverChannel = future.channel();
            logger.info("Redis服务已启动，监听端口: " + port);

            future.channel().closeFuture().addListener((ChannelFutureListener) f -> {
                if (f.isSuccess()) {
                    logger.info("服务器正常关闭");
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
        if (serverChannel != null) {
            serverChannel.close().syncUninterruptibly();
        }
        if (commandExecutor != null) {
            commandExecutor.shutdownGracefully();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        logger.info("Redis服务已完全关闭");
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
