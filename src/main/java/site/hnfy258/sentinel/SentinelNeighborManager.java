package site.hnfy258.sentinel;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.log4j.Logger;
import site.hnfy258.coder.MyDecoder;
import site.hnfy258.coder.MyResponseEncoder;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

/**
 * Sentinel邻居管理器，负责管理和邻居Sentinel的连接
 */
public class SentinelNeighborManager {
    private final Logger logger = Logger.getLogger(SentinelNeighborManager.class);
    
    // 存储邻居Sentinel信息
    private final Map<String, Sentinel> neighborSentinels = new ConcurrentHashMap<>();
    
    // 存储与邻居Sentinel的通道
    private final Map<String, Channel> neighborChannels = new ConcurrentHashMap<>();
    
    // 存储异步请求的映射
    private final Map<String, CompletableFuture<Boolean>> pendingRequests = new ConcurrentHashMap<>();
    
    // 节点最后回复时间
    private final Map<String, Long> lastReplyTime = new ConcurrentHashMap<>();
    
    // 本地Sentinel的信息
    private final String sentinelId;
    private final int quorum;
    
    // 计划任务执行器
    private final ScheduledExecutorService scheduledExecutor;

    // 回调函数，用于创建SentinelClientHandler
    private final BiFunction<Sentinel, String, ChannelHandler> clientHandlerFactory;

    /**
     * 构造函数
     * @param sentinelId 本地Sentinel ID
     * @param quorum 判定下线所需票数
     * @param scheduledExecutor 计划任务执行器
     * @param clientHandlerFactory 客户端处理器工厂
     */
    public SentinelNeighborManager(String sentinelId, int quorum, ScheduledExecutorService scheduledExecutor, 
                                  BiFunction<Sentinel, String, ChannelHandler> clientHandlerFactory) {
        this.sentinelId = sentinelId;
        this.quorum = quorum;
        this.scheduledExecutor = scheduledExecutor;
        this.clientHandlerFactory = clientHandlerFactory;
    }

    /**
     * 添加Sentinel邻居节点
     * @param neighborId 邻居Sentinel ID
     * @param host 邻居Sentinel主机
     * @param port 邻居Sentinel端口
     * @return 操作是否成功
     */
    public boolean addSentinelNeighbor(String neighborId, String host, int port, Sentinel localSentinel) {
        // 如果已经在邻居列表中且连接活跃，直接返回成功
        if (neighborSentinels.containsKey(neighborId)) {
            // 检查连接是否活跃
            Channel channel = neighborChannels.get(neighborId);
            if (channel != null && channel.isActive()) {
                logger.info("已存在活跃的Sentinel邻居连接: " + neighborId);
                return true;
            } else {
                // 如果连接不活跃，尝试重新连接
                logger.info("尝试重新连接到已存在的Sentinel邻居: " + neighborId);
            }
        } else {
            // 创建邻居Sentinel实例（只保存元数据，不需要完整功能）
            Sentinel neighbor = new Sentinel(neighborId, host, port, quorum);
            neighborSentinels.put(neighborId, neighbor);
        }
        
        // 连接到邻居Sentinel
        try {
            connectToSentinel(neighborId, host, port, localSentinel).get(5, TimeUnit.SECONDS);
            
            logger.info("成功连接到Sentinel节点: " + host + ":" + port);
            return true;
        } catch (Exception e) {
            logger.error("添加Sentinel邻居失败: " + neighborId, e);
            return false;
        }
    }

    /**
     * 连接到邻居Sentinel
     * @param neighborId 邻居ID
     * @param host 邻居主机
     * @param port 邻居端口
     * @return 连接Future
     */
    private CompletableFuture<Void> connectToSentinel(String neighborId, String host, int port, Sentinel localSentinel) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        
        if (neighborChannels.containsKey(neighborId) && 
            neighborChannels.get(neighborId).isActive()) {
            return CompletableFuture.completedFuture(null);
        }
        
        // 使用Netty客户端连接到其他Sentinel
        EventLoopGroup group = new NioEventLoopGroup();
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
                        pipeline.addLast(clientHandlerFactory.apply(localSentinel, neighborId));
                    }
                });
        
        // 连接到邻居Sentinel
        connectWithRetry(bootstrap, host, port, future, neighborId, 3, 1000);
        
        return future;
    }

    /**
     * 带重试功能的连接方法
     */
    private void connectWithRetry(Bootstrap bootstrap, String host, int port,
                                  CompletableFuture<Void> future, String neighborId,
                                  int retries, long delayMs) {
        bootstrap.connect(host, port).addListener((ChannelFutureListener) f -> {
            if (f.isSuccess()) {
                Channel channel = f.channel();
                neighborChannels.put(neighborId, channel);
                if (logger.isDebugEnabled()) {
                    logger.debug("连接到其他Sentinel成功: " + host + "/" + channel.remoteAddress());
                }
                future.complete(null);
            } else if (retries > 0) {
                if (logger.isDebugEnabled()) {
                    logger.debug("连接到Sentinel " + host + ":" + port + " 失败，剩余重试次数: " + retries);
                }
                f.channel().eventLoop().schedule(() ->
                                connectWithRetry(bootstrap, host, port, future, neighborId, retries - 1, delayMs),
                        delayMs, TimeUnit.MILLISECONDS);
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("连接到Sentinel失败，已达到最大重试次数", f.cause());
                }
                future.completeExceptionally(f.cause());
            }
        });
    }

    /**
     * 向所有邻居发送PING命令
     */
    public void pingAllNeighbors() {
        // PING命令
        RespArray pingCommand = new RespArray(new Resp[]{
                new BulkString(new BytesWrapper("PING".getBytes(BytesWrapper.CHARSET)))
        });

        // PING所有邻居Sentinel
        for (Map.Entry<String, Channel> entry : neighborChannels.entrySet()) {
            Channel channel = entry.getValue();
            if (channel.isActive()) {
                try {
                    channel.writeAndFlush(pingCommand);
                    // 不在这里更新lastReplyTime，而是等待响应后更新
                } catch (Exception e) {
                    logger.error("PING Sentinel失败: " + entry.getKey(), e);
                }
            }
        }
    }

    /**
     * 向邻居Sentinel询问节点状态
     * @param neighborId 邻居Sentinel ID
     * @param masterId 要询问的主节点ID
     * @return 节点是否下线
     */
    public CompletableFuture<Boolean> askNeighborAboutNodeAsync(String neighborId, String masterId) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        Channel channel = neighborChannels.get(neighborId);
        if (channel == null || !channel.isActive()) {
            future.complete(false);
            return future;
        }

        // 生成唯一请求ID
        String requestId = UUID.randomUUID().toString();

        // 存储请求与Future的映射
        pendingRequests.put(requestId, future);

        // 创建带请求ID的命令
        RespArray isDownCommand = new RespArray(new Resp[]{
                new BulkString(new BytesWrapper("SENTINEL".getBytes(BytesWrapper.CHARSET))),
                new BulkString(new BytesWrapper("IS-MASTER-DOWN-BY-ADDR".getBytes(BytesWrapper.CHARSET))),
                new BulkString(new BytesWrapper(masterId.getBytes(BytesWrapper.CHARSET))),
                new BulkString(new BytesWrapper(requestId.getBytes(BytesWrapper.CHARSET)))
        });

        channel.writeAndFlush(isDownCommand);

        // 设置超时
        scheduledExecutor.schedule(() -> {
            if (!future.isDone()) {
                future.complete(false);
                pendingRequests.remove(requestId);
            }
        }, 2, TimeUnit.SECONDS);

        return future;
    }

    /**
     * 更新邻居的最后回复时间
     * @param neighborId 邻居ID
     */
    public void updateNeighborLastReplyTime(String neighborId) {
        long currentTime = System.currentTimeMillis();
        lastReplyTime.put(neighborId, currentTime);
    }

    /**
     * 获取待处理的请求
     * @param requestId 请求ID
     * @return 与请求关联的Future
     */
    public CompletableFuture<Boolean> getPendingRequest(String requestId) {
        return pendingRequests.remove(requestId);
    }

    /**
     * 移除邻居Sentinel的通道
     * @param neighborId 邻居ID
     */
    public void removeNeighborChannel(String neighborId) {
        neighborChannels.remove(neighborId);
    }

    /**
     * 获取邻居Sentinel列表
     * @return 邻居列表
     */
    public Map<String, Sentinel> getNeighborSentinels() {
        return neighborSentinels;
    }

    /**
     * 获取邻居通道
     * @return 通道列表
     */
    public Map<String, Channel> getNeighborChannels() {
        return neighborChannels;
    }

    /**
     * 关闭所有邻居连接
     */
    public void disconnectAllNeighbors() {
        for (Map.Entry<String, Channel> entry : neighborChannels.entrySet()) {
            try {
                Channel channel = entry.getValue();
                if (channel != null && channel.isActive()) {
                    channel.close().sync();
                    logger.info("与其他Sentinel的连接关闭: " + channel.remoteAddress());
                }
            } catch (Exception e) {
                logger.error("关闭Sentinel连接失败: " + entry.getKey(), e);
            }
        }
    }
} 