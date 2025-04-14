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
import site.hnfy258.aof.AOFSyncStrategy;
import site.hnfy258.cluster.ClusterClient;
import site.hnfy258.cluster.ClusterNode;
import site.hnfy258.cluster.RedisCluster;
import site.hnfy258.coder.MyCommandHandler;
import site.hnfy258.coder.MyDecoder;
import site.hnfy258.coder.MyResponseEncoder;
import site.hnfy258.channel.DefaultChannelSelectStrategy;
import site.hnfy258.channel.LocalChannelOption;
import site.hnfy258.aof.AOFHandler;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.protocal.*;
import site.hnfy258.rdb.core.RDBHandler;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MyRedisService implements RedisService {
    private static final Logger logger = Logger.getLogger(MyRedisService.class);

    // 通过修改这些标志来开启或关闭AOF和RDB功能
    private static final boolean ENABLE_AOF = false;
    private static final boolean ENABLE_RDB = true;

    // 默认数据库数量，与Redis默认值保持一致
    private static final int DEFAULT_DB_NUM = 16;

    private RedisCluster cluster;
    private ClusterNode currentNode;
    private Map<String, ClusterClient> clusterClients = new ConcurrentHashMap<>();
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

    // 添加方法管理集群连接
    public void addClusterClient(String nodeId, ClusterClient client) {
        clusterClients.put(nodeId, client);
    }

    public ClusterClient getClusterClient(String nodeId) {
        return clusterClients.get(nodeId);
    }

    public void setCluster(RedisCluster cluster) {
        this.cluster = cluster;
    }

    public RedisCluster getCluster() {
        return this.cluster;
    }

    public void setCurrentNode(ClusterNode node) {
        this.currentNode = node;
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
            this.aofHandler = new AOFHandler("redis.aof");
            this.aofHandler.setSyncStrategy(AOFSyncStrategy.EVERYSEC);
        } else {
            this.aofHandler = null;
        }
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
            this.commandHandler = new MyCommandHandler(redisCore, aofHandler, rdbHandler);

            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(channelOption.getChannelClass())
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast(new MyDecoder());
                            pipeline.addLast(new MyResponseEncoder());
                            pipeline.addLast(commandExecutor, commandHandler);
                        }
                    });

            // 改为异步绑定
            bootstrap.bind(port).addListener((ChannelFuture future) -> {
                if (future.isSuccess()) {
                    this.serverChannel = future.channel();
                    System.out.println("Redis服务已启动，监听端口: " + port);

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

    public void sendMessageToNode(String toNodeId, Resp message) {
        ClusterClient client = clusterClients.get(toNodeId);
        if (client != null && client.isActive()) {  // 确保连接活跃
            client.sendMessage(message);
        } else {
            System.err.println("No active connection to node " + toNodeId);
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
        return this.currentNode;
    }

    public MyCommandHandler getCommandHandler() {
        return this.commandHandler;
    }
}
