package site.hnfy258.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutorGroup;
import org.apache.log4j.Logger;
import site.hnfy258.RedisCore;
import site.hnfy258.RedisCoreImpl;
import site.hnfy258.aof.AOFSyncStrategy;
import site.hnfy258.cluster.ClusterNode;
import site.hnfy258.cluster.RedisCluster;
import site.hnfy258.cluster.replication.ReplicationHandler;
import site.hnfy258.coder.*;
import site.hnfy258.channel.DefaultChannelSelectStrategy;
import site.hnfy258.channel.LocalChannelOption;
import site.hnfy258.aof.AOFHandler;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.protocal.*;
import site.hnfy258.rdb.core.RDBHandler;

import java.io.IOException;
import java.util.Map;

public class MyRedisService implements RedisService {
    private static final Logger logger = Logger.getLogger(MyRedisService.class);

    // 通过修改这些标志来开启或关闭AOF和RDB功能
    private static final boolean ENABLE_AOF = false;
    private static final boolean ENABLE_RDB = false;
    private static final boolean ENABLE_REPLICATION = true;

    private static final boolean ENABLE_COMPRESSION = false;

    // 默认数据库数量，与Redis默认值保持一致
    private static final int DEFAULT_DB_NUM = 16;


    private RedisCluster cluster;
    private ClusterNode currentNode;
    
    public MyCommandHandler commandHandler;

    private final int port;
    private final RedisCore redisCore;
    private final LocalChannelOption channelOption;
    private final EventExecutorGroup commandExecutor;
    private final AOFHandler aofHandler;
    private final RDBHandler rdbHandler;

    private Channel serverChannel;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    private ReplicationHandler replicationHandler;

    public void setCluster(RedisCluster cluster) {
        this.cluster = cluster;
    }

    public RedisCluster getCluster() {
        return this.cluster;
    }

    public void setCurrentNode(ClusterNode node) {
        this.currentNode = node;

        // 根据节点类型和集群配置初始化复制处理器
        if (ENABLE_REPLICATION && node != null && node.isMaster()) {
            // 无论是分片模式还是非分片模式，都使用相同的ReplicationHandler初始化
            this.replicationHandler = new ReplicationHandler(node);
            if (cluster != null && cluster.isShardingEnabled()) {
                logger.debug("主节点 " + node.getId() + " 在分片模式下初始化复制处理器");
            } else {
                logger.debug("主节点 " + node.getId() + " 在主从模式下初始化复制处理器");
            }
            
            // 更新命令处理器中的复制处理器引用
            if (commandHandler != null) {
                commandHandler.setReplicationHandler(this.replicationHandler);
                logger.debug("已更新命令处理器中的复制处理器引用");
            }
        } else if (node != null) {
            // 如果节点不是主节点，清除复制处理器
            if (this.replicationHandler != null) {
                logger.debug("节点 " + node.getId() + " 不再是主节点，清除复制处理器");
                this.replicationHandler = null;

                // 同步更新命令处理器中的复制处理器引用
                if (commandHandler != null) {
                    commandHandler.setReplicationHandler(null);
                }
            }
            logger.debug("节点 " + node.getId() + " 是从节点或未启用复制功能");
        }
    }

    public MyRedisService(int port) throws IOException {
        this(port, DEFAULT_DB_NUM);
    }

    public MyRedisService(int port, int dbNum) throws IOException {
        this.port = port;
        this.redisCore = new RedisCoreImpl(dbNum, this);
        this.channelOption = new DefaultChannelSelectStrategy().select();
        this.commandExecutor = new DefaultEventExecutorGroup(1,
                new DefaultThreadFactory("redis-cmd"));

        // 根据配置决定是否初始化RDB处理器
        if (ENABLE_RDB) {
            this.rdbHandler = new RDBHandler(redisCore);
            ((RedisCoreImpl) redisCore).setRDBHandler(this.rdbHandler);
        } else {
            this.rdbHandler = null;
        }

        // 根据配置决定是否初始化AOF处理器
        if (ENABLE_AOF) {
            this.aofHandler = new AOFHandler("redis.aof", redisCore);
            this.aofHandler.setSyncStrategy(AOFSyncStrategy.EVERYSEC);
        } else {
            this.aofHandler = null;
        }

        this.replicationHandler = null;
    }

    @Override
    public void start() {
        this.bossGroup = channelOption.boss();
        this.workerGroup = channelOption.selectors();

        try {
            if (ENABLE_RDB && rdbHandler != null) {
                this.rdbHandler.initialize();
            }

            if (ENABLE_AOF && aofHandler != null) {
                this.aofHandler.start();
                this.aofHandler.load(redisCore);
            }

            // 创建统一的命令处理器
            this.commandHandler = new MyCommandHandler(redisCore, aofHandler, rdbHandler, replicationHandler);

            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(channelOption.getChannelClass()).
                    childOption(ChannelOption.SO_KEEPALIVE, true).
                    childOption(ChannelOption.TCP_NODELAY, true).
                    childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline pipeline = ch.pipeline();


                            pipeline.addLast(new MyDecoder());

                            pipeline.addLast(new MyResponseEncoder());

                            if(ENABLE_COMPRESSION){
                                pipeline.addLast(new CompressionCoedC());
                            }
                            pipeline.addLast(commandExecutor, commandHandler);
                            pipeline.addLast(new GlobalExceptionHandler());
                        }
                    });

            // 改为异步绑定
            bootstrap.bind(port).addListener((ChannelFuture future) -> {
                if (future.isSuccess()) {
                    this.serverChannel = future.channel();
                    logger.info("Redis服务已启动，监听端口: " + port);

                    // 添加关闭监听器
                    future.channel().closeFuture().addListener(closeFuture -> {
                        if (!closeFuture.isSuccess()) {
                            logger.error("服务器异常关闭", closeFuture.cause());
                        }
                    });
                } else {
                    logger.error("启动服务失败，端口: " + port, future.cause());
                }
            });
        } catch (Exception e) {
            logger.error("服务器启动异常", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        // 先关闭网络相关资源
        if (serverChannel != null) {
            serverChannel.close().awaitUninterruptibly();
        }

        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }

        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }

        // 然后关闭持久化相关
        if (ENABLE_RDB && rdbHandler != null) {
            rdbHandler.save().exceptionally(e -> {
                logger.error("RDB保存失败", e);
                return null;
            });
            rdbHandler.shutdown();
        }

        if (ENABLE_AOF && aofHandler != null) {
            aofHandler.stop();
        }

        if (commandExecutor != null) {
            commandExecutor.shutdownGracefully();
        }
    }

    /**
     * 向指定节点发送消息
     * @param toNodeId 目标节点ID
     * @param message 要发送的消息
     * @return 是否发送成功
     */
    public boolean sendMessageToNode(String toNodeId, Resp message) {
        // 使用集群的sendMessage方法
        if (currentNode != null && cluster != null) {
            return cluster.sendMessage(currentNode.getId(), toNodeId, message);
        }
        return false;
    }

    /**
     * 向所有从节点广播消息
     * @param message 要广播的消息
     */
    public void broadcastToSlaves(Resp message) {
        if (currentNode != null && currentNode.isMaster() && currentNode.getSlaves() != null) {
            for (ClusterNode slave : currentNode.getSlaves()) {
                if (slave != null) {
                    sendMessageToNode(slave.getId(), message);
                }
            }
        }
    }


    @Override
    public MyRedisService getRedisService() {
        return this;
    }

    @Override
    public RedisCore getRedisCore() {
        return redisCore;
    }

    public int getPort() {
        return port;
    }

    public Resp executeCommand(RespArray commandArray) {
        return commandHandler.processCommand(commandArray);
    }

    public ClusterNode getCurrentNode() {
        return currentNode;
    }

    public AOFHandler getAofHandler() {
        return aofHandler;
    }

    private String formatMessage(Resp message) {
        if (message == null) {
            return "null";
        }
        
        if (message instanceof RespArray) {
            RespArray array = (RespArray) message;
            Resp[] respArray = array.getArray();
            
            if (respArray.length == 0) {
                return "[]";
            }
            
            StringBuilder sb = new StringBuilder();
            if (respArray[0] instanceof BulkString) {
                // 获取命令名称
                String cmdName = ((BulkString) respArray[0]).getContent().toUtf8String();
                sb.append(cmdName.toUpperCase());
                
                // 添加参数（最多显示3个）
                for (int i = 1; i < respArray.length && i < 4; i++) {
                    sb.append(" ");
                    if (respArray[i] instanceof BulkString) {
                        BytesWrapper content = ((BulkString) respArray[i]).getContent();
                        if (content == null) {
                            sb.append("(nil)");
                        } else {
                            // 如果参数太长，截断显示
                            String paramStr = content.toUtf8String();
                            if (paramStr.length() > 30) {
                                sb.append(paramStr.substring(0, 27)).append("...");
                            } else {
                                sb.append(paramStr);
                            }
                        }
                    } else {
                        sb.append(respArray[i].toString());
                    }
                }
                
                // 如果有更多参数，显示省略号
                if (respArray.length > 4) {
                    sb.append(" ...");
                }
                
                // 显示参数总数
                sb.append(" (共").append(respArray.length - 1).append("个参数)");
            } else {
                sb.append("RespArray[");
                for (int i = 0; i < respArray.length && i < 3; i++) {
                    if (i > 0) sb.append(", ");
                    sb.append(respArray[i].toString());
                }
                if (respArray.length > 3) {
                    sb.append("...");
                }
                sb.append("]");
            }
            
            return sb.toString();
        } else if (message instanceof SimpleString) {
            return "SimpleString: " + ((SimpleString) message).getContent();
        } else if (message instanceof BulkString) {
            BytesWrapper content = ((BulkString) message).getContent();
            if (content == null) {
                return "BulkString: (nil)";
            }
            
            String strContent = content.toUtf8String();
            if (strContent.length() > 50) {
                return "BulkString: " + strContent.substring(0, 47) + "...";
            }
            return "BulkString: " + strContent;
        } else if (message instanceof Errors) {
            return "Error: " + ((Errors) message).getContent();
        } else {
            return message.toString();
        }
    }
}