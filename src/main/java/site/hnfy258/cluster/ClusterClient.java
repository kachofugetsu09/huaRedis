package site.hnfy258.cluster;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.log4j.Logger;
import site.hnfy258.RedisCore;
import site.hnfy258.coder.MyCommandHandler;
import site.hnfy258.coder.MyDecoder;
import site.hnfy258.coder.MyResponseEncoder;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Errors;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class ClusterClient {

    static Logger logger = Logger.getLogger(ClusterClient.class);
    private final String host;
    private final int port;
    private Channel channel;
    private EventLoopGroup group;

    private RedisCore redisCore;

    public ClusterClient(String host, int port,RedisCore redisCore) {
        this.host = host;
        this.port = port;
        this.redisCore = redisCore;
    }

    public RedisCore getRedisCore() {
        return redisCore;
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
                        pipeline.addLast(new ClusterClientHandler(redisCore));
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
        private final RedisCore redisCore;

        public ClusterClientHandler(RedisCore redisCore) {
            this.redisCore = redisCore;
        }
        protected void channelRead0(ChannelHandlerContext ctx, Resp msg) {
            if (msg instanceof RespArray) {
                RespArray array = (RespArray) msg;
                Resp response = processCommand(array);
                if (response != null) {
                    ctx.writeAndFlush(response);
                }
            } else {
                ctx.writeAndFlush(new Errors("ERR unknown request type"));
            }
        }

        public Resp processCommand(RespArray commandArray) {
            if (commandArray.getArray().length == 0) {
                return new Errors("ERR empty command");
            }

            try {
                Resp[] array = commandArray.getArray();
                String commandName = ((BulkString) array[0]).getContent().toUtf8String().toUpperCase();
                CommandType commandType;

                try {
                    commandType = CommandType.valueOf(commandName);
                } catch (IllegalArgumentException e) {
                    return new Errors("ERR unknown command '" + commandName + "'");
                }


                Command command = commandType.getSupplier().apply(redisCore);
                command.setContext(array);
                System.out.println("commandName:"+commandName);
                Resp result = command.handle();


                return result;
            } catch (Exception e) {
                logger.error("Error processing command", e);
                return new Errors("ERR " + e.getMessage());
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            System.err.println("Exception in ClusterClientHandler: " + cause.getMessage());
            // Don't close the connection on every exception
            if (cause instanceof java.io.IOException) {
                System.err.println("IO Exception in cluster communication, closing channel");
                ctx.close();
            } else {
                cause.printStackTrace();
            }
        }
    }
}
