package site.hnfy258.sentinel;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.log4j.Logger;
import site.hnfy258.cluster.ClusterNode;
import site.hnfy258.coder.MyDecoder;
import site.hnfy258.coder.MyResponseEncoder;
import site.hnfy258.protocal.*;

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
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    private int quorum;
    private ScheduledExecutorService scheduledExecutor;
    
    // 节点管理器
    private SentinelNodeManager nodeManager;
    
    // 邻居管理器
    private SentinelNeighborManager neighborManager;
    
    // 投票统计
    private Map<String, Map<String, Boolean>> sentinelOpinons = new ConcurrentHashMap<>();

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

        this.scheduledExecutor = Executors.newScheduledThreadPool(4);
        
        // 初始化节点管理器
        this.nodeManager = new SentinelNodeManager();
        
        // 初始化邻居管理器
        this.neighborManager = new SentinelNeighborManager(
            sentinelId, 
            quorum, 
            scheduledExecutor,
            (sentinel, neighborId) -> new SentinelClientHandler(sentinel, neighborId)
        );
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
        return nodeManager.monitorMaster(masterId, ip, port, (resp) -> {
            nodeManager.handleNodeResponse(masterId, resp);
        });
    }

    /**
     * 添加要监控的从节点
     * @param slaveId 从节点ID
     * @param ip 从节点IP
     * @param port 从节点端口
     * @return 操作是否成功
     */
    public boolean monitorSlave(String slaveId, String ip, int port) {
        return nodeManager.monitorSlave(slaveId, ip, port, (resp) -> {
            nodeManager.handleNodeResponse(slaveId, resp);
        });
    }

    /**
     * 添加要监控的Redis节点（支持同时监控主节点和从节点）
     * @param masterNode 主节点
     * @param slaveNodes 从节点列表
     * @return 操作是否成功
     */
    public boolean monitorCluster(ClusterNode masterNode, List<ClusterNode> slaveNodes) {
        return nodeManager.monitorCluster(masterNode, slaveNodes, (resp) -> {
            String nodeId = masterNode != null ? masterNode.getId() : "";
            nodeManager.handleNodeResponse(nodeId, resp);
        });
    }

    /**
     * 添加Sentinel邻居节点
     * @param neighborId 邻居Sentinel ID
     * @param host 邻居Sentinel主机
     * @param port 邻居Sentinel端口
     * @return 操作是否成功
     */
    public boolean addSentinelNeighbor(String neighborId, String host, int port) {
        return neighborManager.addSentinelNeighbor(neighborId, host, port, this);
    }

    /**
     * 向所有连接发送PING命令
     */
    private void pingAllConnections() {
        // PING所有Redis节点
        nodeManager.pingAllNodes();
        
        // PING所有邻居Sentinel
        neighborManager.pingAllNeighbors();
    }

    /**
     * 检查主节点状态
     */
    private void checkMasterStatus() {
        // 使用节点管理器检查节点状态
        nodeManager.checkNodeStatus();
    }

    /**
     * 与其他Sentinel交换信息以确定节点是否客观下线
     */
    private void exchangeSentinelInfo() {
        // 主观下线节点为空，无需交换信息
        Set<String> subjectiveDownNodes = nodeManager.getSubjectiveDownNodes();
        if (subjectiveDownNodes.isEmpty()) {
            return;
        }
        
        // 每次只处理一个主观下线的节点，避免同时处理多个节点
        String nodeToProcess = subjectiveDownNodes.iterator().next();
        
        sentinelOpinons.putIfAbsent(nodeToProcess, new ConcurrentHashMap<>());
        sentinelOpinons.get(nodeToProcess).put(sentinelId, true); // 本地sentinel认为节点下线

        // 遍历所有邻居sentinel询问节点状态
        Map<String, Sentinel> neighborSentinels = neighborManager.getNeighborSentinels();
        Map<String, Channel> neighborChannels = neighborManager.getNeighborChannels();
        
        for (String neighborId : neighborSentinels.keySet()) {
            // 检查与邻居的连接是否活跃
            Channel channel = neighborChannels.get(neighborId);
            if (channel == null || !channel.isActive()) {
                // 如果连接不活跃，尝试重连
                Sentinel neighbor = neighborSentinels.get(neighborId);
                if (neighbor != null) {
                    try {
                        addSentinelNeighbor(neighborId, neighbor.getHost(), neighbor.getPort());
                        channel = neighborChannels.get(neighborId);
                    } catch (Exception e) {
                        logger.debug("重连邻居Sentinel失败: " + neighborId);
                        continue; // 继续处理下一个邻居
                    }
                }
            }
            
            try {
                // 使用异步方法询问邻居节点状态
                CompletableFuture<Boolean> isDownFuture = neighborManager.askNeighborAboutNodeAsync(neighborId, nodeToProcess);
                
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
        ClusterNode node = nodeManager.getMonitoredNodes().get(nodeToProcess);
        boolean isMasterNode = node != null && node.isMaster();
        
        if (downVotes >= quorum) {
            nodeManager.markNodeAsObjectiveDown(nodeToProcess);
        } else {
            nodeManager.removeNodeFromObjectiveDown(nodeToProcess);
        }
    }

    /**
     * 更新节点的最后回复时间
     * @param nodeId 节点ID
     */
    public void updateNodeLastReplyTime(String nodeId) {
        // 先检查是否是被监控的节点
        if (nodeManager.getMonitoredNodes().containsKey(nodeId)) {
            nodeManager.updateNodeLastReplyTime(nodeId);
        } else {
            // 否则尝试更新邻居Sentinel的回复时间
            neighborManager.updateNeighborLastReplyTime(nodeId);
        }
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
            nodeManager.disconnectAllNodes();
            
            // 关闭所有邻居Sentinel连接
            neighborManager.disconnectAllNeighbors();
            
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

    // Getter方法
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
        return nodeManager.getSubjectiveDownNodes();
    }

    public Set<String> getObjectiveDownNodes() {
        return nodeManager.getObjectiveDownNodes();
    }

    public Map<String, ClusterNode> getMonitoredNodes() {
        return nodeManager.getMonitoredNodes();
    }

    /**
     * 获取节点管理器
     */
    public SentinelNodeManager getNodeManager() {
        return nodeManager;
    }

    /**
     * 获取邻居管理器
     */
    public SentinelNeighborManager getNeighborManager() {
        return neighborManager;
    }
}
