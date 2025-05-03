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
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.datatype.RedisData;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Errors;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;
import site.hnfy258.protocal.SimpleString;

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
        private int currentDbIndex = 0;

        public ClusterClientHandler(RedisCore redisCore) {
            this.redisCore = redisCore;
        }
        
        @Override
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
            Resp[] array = commandArray.getArray();
            if (array.length == 0) {
                return new Errors("ERR empty command");
            }
            
            try {
                String commandName = ((BulkString) array[0]).getContent().toUtf8String().toUpperCase();
                
                // 对于频繁执行的命令，减少日志输出
                boolean isFrequentCommand = "PING".equals(commandName) || 
                                           "INFO".equals(commandName) || 
                                           "SCAN".equals(commandName);
                
                // 始终记录写命令的处理
                boolean isImportantCommand = commandName.equals("SET") || 
                                            commandName.equals("ZADD") || 
                                            commandName.equals("RPUSH") ||
                                            commandName.equals("LPUSH") ||
                                            commandName.equals("SADD") ||
                                            commandName.equals("HSET") ||
                                            commandName.equals("DEL") ||
                                            commandName.equals("RPOP") ||
                                            commandName.equals("LPOP");
                
                // 记录详细日志，特别是写命令
                if (!isFrequentCommand || isImportantCommand) {
                    logger.info("从节点收到命令: " + formatCommand(commandArray) + 
                                ", 当前DB: " + currentDbIndex + 
                                ", RedisCore DB: " + redisCore.getCurrentDBIndex());
                }
                
                CommandType commandType;

                try {
                    commandType = CommandType.valueOf(commandName);
                } catch (IllegalArgumentException e) {
                    return new Errors("ERR unknown command '" + commandName + "'");
                }

                // 特殊处理SELECT命令
                if (commandType == CommandType.SELECT) {
                    if (array.length > 1 && array[1] instanceof BulkString) {
                        try {
                            // 获取新的数据库索引
                            String indexStr = ((BulkString) array[1]).getContent().toUtf8String();
                            int newDbIndex = Integer.parseInt(indexStr);
                            
                            // 更新当前索引
                            currentDbIndex = newDbIndex;
                            
                            // 执行SELECT命令
                            redisCore.selectDB(currentDbIndex);
                            logger.info("从节点切换到数据库: " + currentDbIndex);
                            
                            return new SimpleString("OK");
                        } catch (NumberFormatException e) {
                            logger.error("无效的数据库索引", e);
                            return new Errors("ERR invalid DB index");
                        } catch (IllegalArgumentException e) {
                            logger.error("数据库索引超出范围", e);
                            return new Errors("ERR invalid DB index");
                        }
                    } else {
                        return new Errors("ERR wrong number of arguments for 'select' command");
                    }
                }
                
                // 对于所有其他命令，始终强制确保在正确的数据库上下文中
                try {
                    // 获取Redis核心当前数据库索引
                    int currentRedisDbIndex = redisCore.getCurrentDBIndex();
                    
                    // 确保Redis核心处于正确的数据库上下文
                    if (currentRedisDbIndex != currentDbIndex) {
                        logger.info("从节点数据库索引不一致，强制同步: " + currentRedisDbIndex + " -> " + currentDbIndex);
                        redisCore.selectDB(currentDbIndex);
                    }
                } catch (Exception e) {
                    logger.error("切换数据库失败，恢复到默认数据库0: " + e.getMessage(), e);
                    currentDbIndex = 0;
                    redisCore.selectDB(0);
                }
                
                // 执行命令
                if (!isFrequentCommand || isImportantCommand) {
                    logger.info("从节点开始执行命令: " + commandName + 
                                ", 参数长度: " + array.length + 
                                ", 当前DB: " + currentDbIndex);
                }
                
                // 对于写命令，打印额外调试信息
                if (isImportantCommand) {
                    try {
                        if (array.length > 1) {
                            String keyStr = ((BulkString) array[1]).getContent().toUtf8String();
                            Object currentValue = redisCore.get(((BulkString) array[1]).getContent());
                            logger.info("写命令前数据状态: key=" + keyStr + 
                                       ", 在DB:" + currentDbIndex + 
                                       ", 当前值类型: " + (currentValue != null ? currentValue.getClass().getSimpleName() : "null"));
                        }
                    } catch (Exception e) {
                        logger.warn("读取键值数据失败: " + e.getMessage());
                    }
                }
                
                // 执行实际命令
                Command command = commandType.getSupplier().apply(redisCore);
                command.setContext(array);
                Resp result = command.handle();
                
                // 对于写命令，记录命令执行后的状态
                if (isImportantCommand) {
                    try {
                        if (array.length > 1) {
                            String keyStr = ((BulkString) array[1]).getContent().toUtf8String();
                            Object updatedValue = redisCore.get(((BulkString) array[1]).getContent());
                            logger.info("写命令后数据状态: key=" + keyStr + 
                                       ", 在DB:" + currentDbIndex + 
                                       ", 更新后值类型: " + (updatedValue != null ? updatedValue.getClass().getSimpleName() : "null") +
                                       ", 命令结果类型: " + result.getClass().getSimpleName());
                        }
                    } catch (Exception e) {
                        logger.warn("读取更新后键值数据失败: " + e.getMessage());
                    }
                }
                
                // 只有非频繁执行的命令才记录执行完成日志
                if (!isFrequentCommand) {
                    logger.info("从节点执行命令完成: " + commandName + ", 结果类型: " + result.getClass().getSimpleName());
                }
                
                // 命令执行完后，再次检查数据库索引，确保没有意外变化
                int afterCmdDbIndex = redisCore.getCurrentDBIndex();
                if (afterCmdDbIndex != currentDbIndex) {
                    logger.warn("命令执行导致数据库索引变化: " + currentDbIndex + " -> " + afterCmdDbIndex + "，正在恢复");
                    redisCore.selectDB(currentDbIndex);
                }
                
                return result;
            } catch (Exception e) {
                logger.error("处理命令时出错: " + e.getMessage(), e);
                e.printStackTrace();
                return new Errors("ERR " + e.getMessage());
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            System.err.println("Exception in ClusterClientHandler: " + cause.getMessage());
            if (cause instanceof java.io.IOException) {
                System.err.println("IO Exception in cluster communication, closing channel");
                ctx.close();
            } else {
                cause.printStackTrace();
            }
        }
    }

    // 辅助方法：将命令数组转换为可读字符串
    private static String formatCommand(RespArray commandArray) {
        if (commandArray == null || commandArray.getArray().length == 0) {
            return "[]";
        }
        
        Resp[] array = commandArray.getArray();
        StringBuilder sb = new StringBuilder();
        
        // 尝试获取命令名称
        if (array[0] instanceof BulkString) {
            String cmdName = ((BulkString) array[0]).getContent().toUtf8String();
            sb.append(cmdName.toUpperCase());
            
            // 添加参数
            for (int i = 1; i < array.length && i < 4; i++) { // 最多显示前3个参数
                if (array[i] instanceof BulkString) {
                    sb.append(" ").append(((BulkString) array[i]).getContent().toUtf8String());
                } else {
                    sb.append(" [非字符串参数]");
                }
            }
            
            // 如果参数太多，显示省略号
            if (array.length > 4) {
                sb.append(" ...");
            }
            
            // 显示总参数数量
            sb.append(" (共").append(array.length - 1).append("个参数)");
        } else {
            sb.append(commandArray.toString());
        }
        
        return sb.toString();
    }
}
