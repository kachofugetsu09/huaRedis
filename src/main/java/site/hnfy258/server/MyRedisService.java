package site.hnfy258.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import site.hnfy258.RedisCore;
import site.hnfy258.RedisCoreImpl;
import site.hnfy258.coder.MyCommandHandler;
import site.hnfy258.coder.MyDecoder;
import site.hnfy258.coder.MyResponseEncoder;
import site.hnfy258.channel.DefaultChannelSelectStrategy;
import site.hnfy258.channel.LocalChannelOption;

public class MyRedisService implements RedisService {
    private Channel serverChannel;
    private EventLoopGroup eventLoop; // 单个事件循环组
    private int port = 6379;
    private final RedisCore redisCore;
    private final DefaultChannelSelectStrategy channelStrategy;

    public MyRedisService(int port) {
        this.port = port;
        this.redisCore = new RedisCoreImpl();
        this.channelStrategy = new DefaultChannelSelectStrategy();
    }

    @Override
    public void start() {
        LocalChannelOption channelOption = channelStrategy.select();

        // 使用同一个事件循环组
        this.eventLoop = channelOption.boss();

        try {
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(eventLoop, eventLoop) // 使用同一个事件循环组
                    .channel(channelOption.getChannelClass())
                    .option(ChannelOption.SO_BACKLOG, 1024)
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast(new MyDecoder());
                            pipeline.addLast(new MyResponseEncoder());
                            pipeline.addLast(new MyCommandHandler(redisCore));
                        }
                    });

            System.out.println("启动Redis服务，使用单线程模式...");
            ChannelFuture future = serverBootstrap.bind(port).sync();
            System.out.println("Redis服务已启动，监听端口: " + port);

            this.serverChannel = future.channel();

            future.channel().closeFuture().addListener(f -> {
                if (f.isSuccess()) {
                    System.out.println("服务器关闭成功");
                } else {
                    System.err.println("服务器关闭异常: " + f.cause());
                }
            });

            future.channel().closeFuture().sync();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        if (eventLoop != null) {
            eventLoop.shutdownGracefully();
        }
        System.out.println("Redis服务已关闭");
    }

    @Override
    public MyRedisService getRedisService() {
        return this;
    }

    public RedisCore getRedisCore() {
        return redisCore;
    }
}
