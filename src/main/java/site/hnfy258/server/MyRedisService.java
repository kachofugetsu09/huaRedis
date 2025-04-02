package site.hnfy258.server;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class MyRedisService implements RedisService{
    private Channel channel;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private int port = 6379;

    public MyRedisService(int port) {
        this.port = port;
    }


    @Override
    public void start() {
        bossGroup = new NioEventLoopGroup(1); // 接受连接的线程组
        workerGroup = new NioEventLoopGroup(); // 处理IO的线程组

        try {
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 1024)
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline pipeline = ch.pipeline();
                            // 添加Redis协议解码器
                            pipeline.addLast(new RedisCommandDecoder());
                            // 添加Redis命令处理器
                            pipeline.addLast(new RedisCommandHandler());
                            // 添加Redis响应编码器
                            pipeline.addLast(new RedisResponseEncoder());
                        }
                    });

            // 绑定端口并启动服务
            ChannelFuture future = serverBootstrap.bind(port).sync();
            System.out.println("Redis服务已启动，监听端口: " + port);

            // 保存Channel以便后续关闭
            this.channel = future.channel();

            // 等待服务器套接字关闭
            // future.channel().closeFuture().sync();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Override
    public void close() {
        if (channel != null) {
            channel.close();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        System.out.println("Redis服务已关闭");
    }


    @Override
    public MyRedisService getRedisService() {
        return this;
    }
}
