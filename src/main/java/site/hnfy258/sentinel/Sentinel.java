package site.hnfy258.sentinel;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.log4j.Logger;
import site.hnfy258.RedisCore;
import site.hnfy258.cluster.ClusterNode;
import site.hnfy258.coder.MyDecoder;
import site.hnfy258.coder.MyResponseEncoder;
import site.hnfy258.command.impl.Ping;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.protocal.*;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class Sentinel {

    Logger logger = Logger.getLogger(Sentinel.class);
    private String sentinelId;
    private String host;
    private int port;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;
    private final Map<String, Channel> neighborChannels = new ConcurrentHashMap<>();
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    private Map<String, Sentinel> neighborSentinels;

    private Map<String, ClusterNode> monitoredNodes;

    private Set<String> subjectiveDownNodes;
    private Set<String> objectiveDownNodes;

    private Map<String, Map<String, Boolean>> sentinelOpinons;

    // 存储异步请求的映射
    private final Map<String, CompletableFuture<Boolean>> pendingRequests = new ConcurrentHashMap<>();

    private int quorum;

    private ScheduledExecutorService scheduledExecutor;

    private Map<String, Long> lastReplyTime;



    /**
     * Sentinel构造函数
     * @param sentinelId sentinel唯一ID
     * @param host 监听主机地址
     * @param port 监听端口
     * @param quorum 判定主节点下线所需的票数
     */
    public Sentinel(String sentinelId, String host, int port, int quorum) {
        this.sentinelId = sentinelId;
        this.host = host;
        this.port = port;
        this.quorum = quorum;

        this.neighborSentinels = new ConcurrentHashMap<>();
        this.monitoredNodes = new ConcurrentHashMap<>();
        this.subjectiveDownNodes = ConcurrentHashMap.newKeySet();
        this.objectiveDownNodes = ConcurrentHashMap.newKeySet();
        this.sentinelOpinons = new ConcurrentHashMap<>();
        this.lastReplyTime = new ConcurrentHashMap<>();

        this.scheduledExecutor = Executors.newScheduledThreadPool(4);
    }

    /**
     * 启动Sentinel服务
     * @return 启动是否成功
     */
    public boolean start() {
        if (!isRunning.compareAndSet(false, true)) {
            logger.warn("Sentinel服务已经在运行中");
            return false;
        }

        // 启动Sentinel服务器
        CompletableFuture<Boolean> serverFuture = startServer();
        
        try {
            if (!serverFuture.get(30, TimeUnit.SECONDS)) {
                isRunning.set(false);
                logger.error("启动Sentinel服务器失败");
                return false;
            }
            
            logger.info("Sentinel服务器已启动，监听地址: " + host + ":" + port);
            
            // 启动监控
            startMonitoring();
            
            return true;
        } catch (Exception e) {
            isRunning.set(false);
            logger.error("启动Sentinel服务出错", e);
            return false;
        }
    }

    /**
     * 启动Sentinel网络服务器
     * @return 启动结果Future
     */
    private CompletableFuture<Boolean> startServer() {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        
        try {
            bossGroup = new NioEventLoopGroup(1);
            workerGroup = new NioEventLoopGroup();
            
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(
                                    new MyDecoder(),
                                    new MyResponseEncoder(),
                                    new SentinelServerHandler(Sentinel.this)
                            );
                        }
                    });
            
            // 绑定端口并启动
            bootstrap.bind(host, port).addListener((ChannelFutureListener) channelFuture -> {
                if (channelFuture.isSuccess()) {
                    serverChannel = channelFuture.channel();
                    future.complete(true);
                } else {
                    logger.error("无法绑定到端口 " + port, channelFuture.cause());
                    future.complete(false);
                }
            });
        } catch (Exception e) {
            logger.error("启动Sentinel服务器出错", e);
            future.complete(false);
        }
        
        return future;
    }

    /**
     * 开始监控Redis节点和其他Sentinel
     */
    public void startMonitoring() {
        // 每秒检测一次主节点状态（主观下线检测）
        scheduledExecutor.scheduleAtFixedRate(this::checkMasterStatus, 0, 1, TimeUnit.SECONDS);

        // 每秒与其他Sentinel交换信息（客观下线检测）
        scheduledExecutor.scheduleAtFixedRate(this::exchangeSentinelInfo, 0, 1, TimeUnit.SECONDS);
        
        // 每10秒发送一次PING给所有已连接的节点和Sentinel
        scheduledExecutor.scheduleAtFixedRate(this::pingAllConnections, 0, 10, TimeUnit.SECONDS);
    }

    /**
     * 添加要监控的主节点
     * @param masterId 主节点ID
     * @param ip 主节点IP
     * @param port 主节点端口
     * @return 操作是否成功
     */
    public boolean monitorMaster(String masterId, String ip, int port) {
        // 检查是否已经在监控列表中
        if (monitoredNodes.containsKey(masterId)) {
            ClusterNode existingNode = monitoredNodes.get(masterId);
            // 如果连接断开，尝试重新连接
            if (!existingNode.isActive()) {
                logger.info("尝试重新连接到已存在的节点: " + masterId);
                existingNode.connect();
            }
            
            // 检查回调是否设置
            if (existingNode.getResponseCallback() == null) {
                logger.info("[DEBUG] 已存在节点 " + masterId + " 没有设置回调，重新设置回调");
                existingNode.setResponseCallback((resp) -> {
                    handleNodeResponse(masterId, resp);
                });
            } else {
                logger.info("[DEBUG] 已存在节点 " + masterId + " 已设置回调");
            }
            
            return true;
        }
        
        try {
            logger.info("Sentinel正在连接到节点: " + masterId + " (" + ip + ":" + port + ")");
            
            // 创建新的节点实例
            ClusterNode masterNode = new ClusterNode(masterId, ip, port, true);
            
            // 设置节点响应回调
            logger.info("[DEBUG] 为新节点 " + masterId + " 设置响应回调");
            masterNode.setResponseCallback((resp) -> {
                handleNodeResponse(masterId, resp);
            });
            
            // 连接到主节点
            CompletableFuture<Void> connectFuture = masterNode.connect();
            connectFuture.get(5, TimeUnit.SECONDS);
            
            // 如果连接成功，添加到监控列表
            if (masterNode.isActive()) {
                monitoredNodes.put(masterId, masterNode);
                lastReplyTime.put(masterId, System.currentTimeMillis());
                logger.info("Sentinel成功连接到节点: " + masterId + "，初始化最后回复时间: " + lastReplyTime.get(masterId));
                return true;
            } else {
                logger.error("无法连接到节点: " + masterId);
                return false;
            }
        } catch (Exception e) {
            logger.error("监控主节点失败: " + masterId, e);
            return false;
        }
    }

    /**
     * 添加要监控的从节点
     * @param slaveId 从节点ID
     * @param ip 从节点IP
     * @param port 从节点端口
     * @return 操作是否成功
     */
    public boolean monitorSlave(String slaveId, String ip, int port) {
        // 检查是否已经在监控列表中
        if (monitoredNodes.containsKey(slaveId)) {
            ClusterNode existingNode = monitoredNodes.get(slaveId);
            // 如果连接断开，尝试重新连接
            if (!existingNode.isActive()) {
                logger.info("尝试重新连接到已存在的从节点: " + slaveId);
                existingNode.connect();
            }
            
            // 检查回调是否设置
            if (existingNode.getResponseCallback() == null) {
                logger.info("已存在从节点 " + slaveId + " 没有设置回调，重新设置回调");
                existingNode.setResponseCallback((resp) -> {
                    handleNodeResponse(slaveId, resp);
                });
            }
            
            return true;
        }
        
        try {
            logger.info("Sentinel正在连接到从节点: " + slaveId + " (" + ip + ":" + port + ")");
            
            // 创建新的节点实例（标记为非主节点）
            ClusterNode slaveNode = new ClusterNode(slaveId, ip, port, false);
            
            // 设置节点响应回调
            logger.info("为新从节点 " + slaveId + " 设置响应回调");
            slaveNode.setResponseCallback((resp) -> {
                handleNodeResponse(slaveId, resp);
            });
            
            // 连接到从节点
            CompletableFuture<Void> connectFuture = slaveNode.connect();
            connectFuture.get(5, TimeUnit.SECONDS);
            
            // 如果连接成功，添加到监控列表
            if (slaveNode.isActive()) {
                monitoredNodes.put(slaveId, slaveNode);
                lastReplyTime.put(slaveId, System.currentTimeMillis());
                logger.info("Sentinel成功连接到从节点: " + slaveId + "，初始化最后回复时间: " + lastReplyTime.get(slaveId));
                return true;
            } else {
                logger.error("无法连接到从节点: " + slaveId);
                return false;
            }
        } catch (Exception e) {
            logger.error("监控从节点失败: " + slaveId, e);
            return false;
        }
    }

    /**
     * 添加要监控的Redis节点（支持同时监控主节点和从节点）
     * @param clusterId 集群ID
     * @param masterNode 主节点
     * @param slaveNodes 从节点列表
     * @return 操作是否成功
     */
    public boolean monitorCluster(ClusterNode masterNode, List<ClusterNode> slaveNodes) {
        boolean success = true;
        
        // 监控主节点
        if (masterNode != null) {
            success &= monitorMaster(masterNode.getId(), masterNode.getIp(), masterNode.getPort());
            logger.info("监控主节点: " + masterNode.getId());
        }
        
        // 监控所有从节点
        if (slaveNodes != null) {
            for (ClusterNode slave : slaveNodes) {
                if (slave != null) {
                    success &= monitorSlave(slave.getId(), slave.getIp(), slave.getPort());
                    logger.info("监控从节点: " + slave.getId());
                }
            }
        }
        
        return success;
    }

    private void handleNodeResponse(String nodeId, Resp resp) {
        if (resp instanceof SimpleString) {
            String content = ((SimpleString) resp).getContent();
            if ("PONG".equalsIgnoreCase(content)) {
                // 如果节点已经被标记为下线，但现在收到了PONG，需要重新评估其状态
                boolean wasOffline = subjectiveDownNodes.contains(nodeId) || objectiveDownNodes.contains(nodeId);
                
                // 更新节点最后回复时间
                updateNodeLastReplyTime(nodeId);
                
                // 如果之前被标记为下线状态，但现在收到了PONG，移除下线标记
                if (wasOffline) {
                    logger.info("曾被标记为下线的节点 " + nodeId + " 收到PONG响应，正在恢复其状态");
                    subjectiveDownNodes.remove(nodeId);
                    objectiveDownNodes.remove(nodeId);
                    
                    // 清理该节点的意见记录
                    sentinelOpinons.remove(nodeId);
                }
            }
        }
    }

    /**
     * 添加Sentinel邻居节点
     * @param neighborId 邻居Sentinel ID
     * @param host 邻居Sentinel主机
     * @param port 邻居Sentinel端口
     * @return 操作是否成功
     */
    public boolean addSentinelNeighbor(String neighborId, String host, int port) {
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
            connectToSentinel(neighborId, host, port).get(5, TimeUnit.SECONDS);
            
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
    private CompletableFuture<Void> connectToSentinel(String neighborId, String host, int port) {
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
                        pipeline.addLast(new SentinelClientHandler(Sentinel.this, neighborId));
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
     * 向所有连接发送PING命令
     */
    private void pingAllConnections() {
        // PING命令
        RespArray pingCommand = new RespArray(new Resp[]{
                new BulkString(new BytesWrapper("PING".getBytes(BytesWrapper.CHARSET)))
        });

        // 只PING未下线的节点
        for (Map.Entry<String, ClusterNode> entry : monitoredNodes.entrySet()) {
            String nodeId = entry.getKey();
            ClusterNode node = entry.getValue();
            
            // 跳过已经标记为下线的节点
            if (subjectiveDownNodes.contains(nodeId) || objectiveDownNodes.contains(nodeId)) {
                continue;
            }
            
            if (node.isActive()) {
                try {
                    node.sendMessage(pingCommand);
                    if (logger.isDebugEnabled()) {
                        logger.debug("已向节点 " + nodeId + " 发送PING");
                    }
                } catch (Exception e) {
                    logger.error("PING节点失败: " + nodeId, e);
                }
            }
        }

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
     * 检查主节点状态
     */
    private void checkMasterStatus() {
        // 记录当前已确认的下线节点ID，避免多个节点同时下线
        String currentDownNodeId = null;
        boolean hasDetectedDownNode = false;

        for (Map.Entry<String, ClusterNode> entry : monitoredNodes.entrySet()) {
            String nodeId = entry.getKey();
            ClusterNode node = entry.getValue();
            
            // 如果当前循环中已有节点被标记下线，其他节点暂时不处理，避免连锁反应
            if (hasDetectedDownNode) {
                continue;
            }
            
            // 获取最后一次回复时间
            Long lastReply = lastReplyTime.get(nodeId);
            long currentTime = System.currentTimeMillis();
            
            // 只在关键节点状态变化时输出日志，减少日常检查的日志
            boolean logDetails = false;
            if (lastReply != null && (currentTime - lastReply) > 4000) {
                // 接近超时时才记录日志
                logDetails = true;
            }
            
            if (logDetails) {
                logger.debug("[检查] 节点 " + nodeId + " 状态: 最后回复时间=" + lastReply + ", 当前时间=" + currentTime + ", 差值=" + (currentTime - lastReply) + "ms");
            }
            
            // 如果节点连接已断开，尝试重新连接
            if (!node.isActive()) {
                try {
                    node.connect().get(1, TimeUnit.SECONDS);
                    
                    if (node.isActive()) {
                        logger.info("成功重新连接到节点: " + nodeId);
                        
                        // 重新设置回调以防丢失
                        if (node.getResponseCallback() == null) {
                            node.setResponseCallback((resp) -> {
                                handleNodeResponse(nodeId, resp);
                            });
                        }
                        
                        long newTime = System.currentTimeMillis();
                        lastReplyTime.put(nodeId, newTime);
                        
                        if (subjectiveDownNodes.contains(nodeId)) {
                            logger.info("节点 " + nodeId + " 恢复上线，从主观下线列表中移除");
                            subjectiveDownNodes.remove(nodeId);
                        }
                        
                        continue;
                    }
                } catch (Exception e) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("重新连接到节点失败: " + nodeId);
                    }
                }
            }
            
            // 如果超过5秒没有回复，认为节点主观下线
            if (lastReply == null || (currentTime - lastReply) > 5000) {
                if (!subjectiveDownNodes.contains(nodeId)) {
                    // 每次检测周期只处理一个节点的下线
                    hasDetectedDownNode = true;
                    currentDownNodeId = nodeId;
                    
                    // 将节点添加到主观下线列表
                    subjectiveDownNodes.add(nodeId);
                    // logger.info("节点 " + nodeId + " 已主观下线，最后回复时间=" + lastReply + ", 超时: " + (lastReply == null ? "null" : (currentTime - lastReply) + "ms"));
                    
                
                }
            } else {
                // 如果节点回复正常
                if (subjectiveDownNodes.contains(nodeId)) {
                    logger.info("[DEBUG] 节点 " + nodeId + " 恢复正常，从主观下线列表中移除");
                    subjectiveDownNodes.remove(nodeId);
                }
            }
        }
    }

    /**
     * 与其他Sentinel交换信息以确定节点是否客观下线
     */
    private void exchangeSentinelInfo() {
        // 主观下线节点为空，无需交换信息
        if (subjectiveDownNodes.isEmpty()) {
            return;
        }
        
        // 每次只处理一个主观下线的节点，避免同时处理多个节点
        String nodeToProcess = subjectiveDownNodes.iterator().next();
        
        sentinelOpinons.putIfAbsent(nodeToProcess, new ConcurrentHashMap<>());
        sentinelOpinons.get(nodeToProcess).put(sentinelId, true); // 本地sentinel认为节点下线

        // 遍历所有邻居sentinel询问节点状态
        for (String neighborId : neighborSentinels.keySet()) {
            // 检查与邻居的连接是否活跃
            Channel channel = neighborChannels.get(neighborId);
            if (channel == null || !channel.isActive()) {
                // 如果连接不活跃，尝试重连
                Sentinel neighbor = neighborSentinels.get(neighborId);
                if (neighbor != null) {
                    try {
                        connectToSentinel(neighborId, neighbor.getHost(), neighbor.getPort())
                            .get(1, TimeUnit.SECONDS); // 短超时快速重连
                        channel = neighborChannels.get(neighborId);
                    } catch (Exception e) {
                        logger.debug("重连邻居Sentinel失败: " + neighborId);
                        continue; // 继续处理下一个邻居
                    }
                }
            }
            
            try {
                // 使用异步方法询问邻居节点状态
                CompletableFuture<Boolean> isDownFuture = askNeighborAboutNodeAsync(neighborId, nodeToProcess);
                
                // 设置一个短暂的超时，避免阻塞主流程
                try {
                    Boolean isDown = isDownFuture.get(500, TimeUnit.MILLISECONDS);
                    sentinelOpinons.get(nodeToProcess).put(neighborId, isDown);
                } catch (TimeoutException e) {
                    // 如果超时，先不考虑这个邻居的意见，后续会通过异步回调更新
                    logger.debug("等待邻居Sentinel " + neighborId + " 响应超时");
                }
            } catch (Exception e) {
                logger.error("询问邻居Sentinel节点状态失败", e);
            }
        }

        // 统计下线票数
        long downVotes = sentinelOpinons.get(nodeToProcess).values().stream().filter(isDown -> isDown).count();

        // 判断节点类型，区分处理主节点和从节点
        ClusterNode node = monitoredNodes.get(nodeToProcess);
        boolean isMasterNode = node != null && node.isMaster();
        
        if (downVotes >= quorum) {
            if (!objectiveDownNodes.contains(nodeToProcess)) {
                objectiveDownNodes.add(nodeToProcess);
                if (isMasterNode) {
                    logger.info("主节点 " + nodeToProcess + " 已确认客观下线");
                    // 注意：这里只处理单个节点的下线，不影响其他节点
                } else {
                    logger.info("从节点 " + nodeToProcess + " 已确认客观下线");
                    // 从节点下线处理，同样不影响其他节点
                }
            }
        } else {
            objectiveDownNodes.remove(nodeToProcess);
        }
    }

    /**
     * 向邻居Sentinel询问节点状态
     * @param neighborId 邻居Sentinel ID
     * @param masterId 要询问的主节点ID
     * @return 节点是否下线
     */
    private CompletableFuture<Boolean> askNeighborAboutNodeAsync(String neighborId, String masterId) {
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
     * 更新节点的最后回复时间
     * @param nodeId 节点ID
     */
    public void updateNodeLastReplyTime(String nodeId) {
        long currentTime = System.currentTimeMillis();
        Long oldTime = lastReplyTime.get(nodeId);
        lastReplyTime.put(nodeId, currentTime);
    }

    /**
     * 关闭Sentinel服务
     */
    public void shutdown() {
        if (isRunning.compareAndSet(true, false)) {
            logger.info("正在关闭Sentinel服务...");
            
            // 关闭所有计划任务
            scheduledExecutor.shutdown();
            
            // 断开与所有节点的连接
            for (Map.Entry<String, ClusterNode> entry : monitoredNodes.entrySet()) {
                try {
                    entry.getValue().disconnect();
                } catch (Exception e) {
                    logger.error("断开节点连接失败: " + entry.getKey(), e);
                }
            }
            
            // 关闭所有邻居Sentinel连接
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
            
            // 关闭服务器
            if (serverChannel != null) {
                try {
                    serverChannel.close().sync();
                } catch (InterruptedException e) {
                    logger.error("关闭服务器通道失败", e);
                }
            }
            
            // 关闭线程组
            if (bossGroup != null) {
                bossGroup.shutdownGracefully();
            }
            if (workerGroup != null) {
                workerGroup.shutdownGracefully();
            }
            
            logger.info("Sentinel服务已关闭");
        }
    }

    /**
     * 处理来自其他Sentinel的客户端连接
     */
    static class SentinelClientHandler extends SimpleChannelInboundHandler<Resp> {
        private final Logger logger = Logger.getLogger(SentinelClientHandler.class);
        private final Sentinel sentinel;
        private final String neighborId;

        public SentinelClientHandler(Sentinel sentinel, String neighborId) {
            this.sentinel = sentinel;
            this.neighborId = neighborId;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Resp msg) {
            if (msg instanceof RespArray) {
                RespArray array = (RespArray) msg;
                processCommand(ctx, array);
                // 尝试处理可能的异步响应
                processAsyncResponse(array);
            } else if (msg instanceof SimpleString) {
                // 处理简单字符串响应，如PONG
                String content = ((SimpleString) msg).getContent();
                if ("PONG".equalsIgnoreCase(content)) {
                    // 收到PONG响应后更新最后回复时间
                    sentinel.updateNodeLastReplyTime(neighborId);
                    if (logger.isDebugEnabled()) {
                        logger.debug("收到Sentinel " + neighborId + " 的PONG响应");
                    }
                }
            }
        }

        private void processCommand(ChannelHandlerContext ctx, RespArray command) {
            try {
                Resp[] array = command.getArray();
                if (array.length == 0) {
                    return;
                }

                // 解析命令
                String cmdName = ((BulkString) array[0]).getContent().toUtf8String().toUpperCase();

                // 处理PING命令
                if ("PING".equals(cmdName)) {
                    ctx.writeAndFlush(new SimpleString("PONG"));
                    sentinel.updateNodeLastReplyTime(neighborId);
                }
                // 处理IS-MASTER-DOWN-BY-ADDR命令
                else if ("SENTINEL".equals(cmdName) && array.length >= 3 && 
                        "IS-MASTER-DOWN-BY-ADDR".equals(((BulkString) array[1]).getContent().toUtf8String())) {
                    String masterId = ((BulkString) array[2]).getContent().toUtf8String();
                    boolean isDown = sentinel.subjectiveDownNodes.contains(masterId);
                    
                    // 检查是否有请求ID
                    String requestId = null;
                    if (array.length >= 4) {
                        requestId = ((BulkString) array[3]).getContent().toUtf8String();
                    }
                    
                    // 返回节点状态
                    RespArray response;
                    if (requestId != null) {
                        response = new RespArray(new Resp[]{
                                new BulkString(new BytesWrapper((isDown ? "1" : "0").getBytes(BytesWrapper.CHARSET))),
                                new BulkString(new BytesWrapper(requestId.getBytes(BytesWrapper.CHARSET)))
                        });
                    } else {
                        response = new RespArray(new Resp[]{
                                new BulkString(new BytesWrapper((isDown ? "1" : "0").getBytes(BytesWrapper.CHARSET)))
                        });
                    }
                    ctx.writeAndFlush(response);
                }
            } catch (Exception e) {
                logger.error("处理来自邻居Sentinel的命令失败", e);
            }
        }

        /**
         * 处理异步响应，完成相应的CompletableFuture
         */
        private void processAsyncResponse(RespArray response) {
            try {
                Resp[] array = response.getArray();
                if (array.length < 2) {
                    return;  // 至少需要结果和请求ID
                }
                
                // 尝试解析响应
                if (array[0] instanceof BulkString && array[1] instanceof BulkString) {
                    String resultValue = ((BulkString) array[0]).getContent().toUtf8String();
                    String requestId = ((BulkString) array[1]).getContent().toUtf8String();
                    
                    // 查找并完成对应的Future
                    CompletableFuture<Boolean> future = sentinel.pendingRequests.remove(requestId);
                    if (future != null && !future.isDone()) {
                        boolean isDown = "1".equals(resultValue);
                        future.complete(isDown);
                        if (logger.isDebugEnabled()) {
                            logger.debug("收到异步IS-MASTER-DOWN-BY-ADDR响应, requestId=" + requestId + ", isDown=" + isDown);
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("处理异步响应失败", e);
            }
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            logger.info("与邻居Sentinel建立连接: " + ctx.channel().remoteAddress());
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            logger.info("与邻居Sentinel连接断开: " + ctx.channel().remoteAddress());
            sentinel.neighborChannels.remove(neighborId);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            logger.error("与邻居Sentinel通信异常", cause);
            ctx.close();
        }
    }

    /**
     * 处理接收到的连接请求
     */
    static class SentinelServerHandler extends SimpleChannelInboundHandler<Resp> {
        private final Logger logger = Logger.getLogger(SentinelServerHandler.class);
        private final Sentinel sentinel;
        private String clientId = null;

        public SentinelServerHandler(Sentinel sentinel) {
            this.sentinel = sentinel;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Resp msg) {
            if (msg instanceof RespArray) {
                RespArray array = (RespArray) msg;
                processCommand(ctx, array);
            } else if (msg instanceof SimpleString) {
                // 处理简单字符串响应，如PONG
                String content = ((SimpleString) msg).getContent();
                if ("PONG".equalsIgnoreCase(content) && clientId != null) {
                    // 收到PONG响应后更新最后回复时间
                    sentinel.updateNodeLastReplyTime(clientId);
                    if (logger.isDebugEnabled()) {
                        logger.debug("收到客户端 " + clientId + " 的PONG响应");
                    }
                }
            }
        }

        private void processCommand(ChannelHandlerContext ctx, RespArray command) {
            try {
                Resp[] array = command.getArray();
                if (array.length == 0) {
                    return;
                }

                // 解析命令
                String cmdName = ((BulkString) array[0]).getContent().toUtf8String().toUpperCase();

                // 处理PING命令
                if ("PING".equals(cmdName)) {
                    ctx.writeAndFlush(new SimpleString("PONG"));
                    if (clientId != null) {
                        sentinel.updateNodeLastReplyTime(clientId);
                    }
                }
                // 处理SENTINEL命令
                else if ("SENTINEL".equals(cmdName)) {
                    handleSentinelCommand(ctx, array);
                }
                // 处理其他命令...
            } catch (Exception e) {
                logger.error("处理命令失败", e);
                ctx.writeAndFlush(new Errors("ERR " + e.getMessage()));
            }
        }

        private void handleSentinelCommand(ChannelHandlerContext ctx, Resp[] array) {
            if (array.length < 2) {
                ctx.writeAndFlush(new Errors("ERR wrong number of arguments for 'sentinel' command"));
                return;
            }

            String subCommand = ((BulkString) array[1]).getContent().toUtf8String().toUpperCase();

            // 处理SENTINEL HELLO命令（注册节点）
            if ("HELLO".equals(subCommand) && array.length >= 3) {
                clientId = ((BulkString) array[2]).getContent().toUtf8String();
                ctx.writeAndFlush(new SimpleString("OK"));
                logger.info("客户端注册ID: " + clientId);
            }
            // 处理IS-MASTER-DOWN-BY-ADDR命令
            else if ("IS-MASTER-DOWN-BY-ADDR".equals(subCommand) && array.length >= 3) {
                String masterId = ((BulkString) array[2]).getContent().toUtf8String();
                boolean isDown = sentinel.subjectiveDownNodes.contains(masterId);
                
                // 检查是否有请求ID
                String requestId = null;
                if (array.length >= 4) {
                    requestId = ((BulkString) array[3]).getContent().toUtf8String();
                }
                
                // 返回节点状态
                RespArray response;
                if (requestId != null) {
                    response = new RespArray(new Resp[]{
                            new BulkString(new BytesWrapper((isDown ? "1" : "0").getBytes(BytesWrapper.CHARSET))),
                            new BulkString(new BytesWrapper(requestId.getBytes(BytesWrapper.CHARSET)))
                    });
                } else {
                    response = new RespArray(new Resp[]{
                            new BulkString(new BytesWrapper((isDown ? "1" : "0").getBytes(BytesWrapper.CHARSET)))
                    });
                }
                ctx.writeAndFlush(response);
            }
            // 处理其他SENTINEL子命令...
            else {
                ctx.writeAndFlush(new Errors("ERR unknown sentinel subcommand '" + subCommand + "'"));
            }
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            logger.info("新连接建立: " + ctx.channel().remoteAddress());
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            logger.info("连接关闭: " + ctx.channel().remoteAddress());
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            logger.error("连接异常", cause);
            ctx.close();
        }
    }

    // Getter和Setter方法
    public String getSentinelId() {
        return sentinelId;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public Set<String> getSubjectiveDownNodes() {
        return subjectiveDownNodes;
    }

    public Set<String> getObjectiveDownNodes() {
        return objectiveDownNodes;
    }

    public Map<String, ClusterNode> getMonitoredNodes() {
        return monitoredNodes;
    }
}
