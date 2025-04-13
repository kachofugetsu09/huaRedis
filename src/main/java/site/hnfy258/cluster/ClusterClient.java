package site.hnfy258.cluster;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import site.hnfy258.coder.MyDecoder;
import site.hnfy258.coder.MyResponseEncoder;
import site.hnfy258.protocal.Resp;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class ClusterClient {
    private final String host;
    private final int port;
    private Channel channel;
    private EventLoopGroup group;

    public ClusterClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public CompletableFuture<Void> connect() {
        CompletableFuture<Void> future = new CompletableFuture<>();

        group = new NioEventLoopGroup();
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new MyDecoder());
                        pipeline.addLast(new MyResponseEncoder());
                        pipeline.addLast(new ClusterClientHandler());
                    }
                });

        connectWithRetry(bootstrap, host, port, future, 3, 1000);
        return future;
    }

    private void connectWithRetry(Bootstrap bootstrap, String host, int port,
                                  CompletableFuture<Void> future, int retries, long delayMs) {
        bootstrap.connect(host, port).addListener((ChannelFutureListener) f -> {
            if (f.isSuccess()) {
                channel = f.channel();
                future.complete(null);
            } else if (retries > 0) {
                System.out.printf("Connection to %s:%d failed, %d retries left. Retrying...%n",
                        host, port, retries);
                f.channel().eventLoop().schedule(() ->
                                connectWithRetry(bootstrap, host, port, future, retries - 1, delayMs),
                        delayMs, TimeUnit.MILLISECONDS);
            } else {
                future.completeExceptionally(f.cause());
            }
        });
    }

    public void sendMessage(Resp resp) {
        if (channel != null && channel.isActive()) {
            channel.writeAndFlush(resp);
        } else {
            System.err.println("Channel is not active. Cannot send message.");
        }
    }

    public void disconnect() {
        if (channel != null) {
            channel.close();
        }
        if (group != null) {
            group.shutdownGracefully();
        }
    }

    public boolean isActive() {
        return channel != null && channel.isActive();
    }

    private static class ClusterClientHandler extends SimpleChannelInboundHandler<Resp> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Resp msg) {
            System.out.println("Received cluster message: " + msg);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            System.err.println("Exception in ClusterClientHandler: " + cause.getMessage());
            ctx.close();
        }
    }
}
