package site.hnfy258.cluster;

import site.hnfy258.RedisCoreImpl;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;
import site.hnfy258.protocal.SimpleString;
import site.hnfy258.server.MyRedisService;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class RedisCluster implements Cluster {
    private final Map<String, ClusterNode> nodes;
    private final Map<String, MyRedisService> services;
    private ShardingStrategy shardingStrategy;
    private boolean shardingEnabled;

    public RedisCluster() {
        this.nodes = new ConcurrentHashMap<>();
        this.services = new ConcurrentHashMap<>();
        this.shardingEnabled = false;
    }

    public RedisCluster(boolean shardingEnabled) {
        this.nodes = new ConcurrentHashMap<>();
        this.services = new ConcurrentHashMap<>();
        this.shardingEnabled = shardingEnabled;
    }

    public void initializeSharding() {
        if (shardingEnabled && !nodes.isEmpty()) {
            List<String> nodeIds = new ArrayList<>(nodes.keySet());
            System.out.println("初始化分片，节点: " + nodeIds);
            this.shardingStrategy = new ConsistentHashSharding(nodeIds);
        }
    }

    @Override
    public void stop() {
        for (MyRedisService service : services.values()) {
            service.close();
        }
        
        // 断开所有节点间的连接
        for (ClusterNode node : nodes.values()) {
            if (node.isActive()) {
                node.disconnect();
            }
        }
    }

    @Override
    public MyRedisService getNode(String nodeId) {
        return services.get(nodeId);
    }

    public void addNode(String nodeId, String ip, int port) throws IOException {
        try {
            // 创建节点并指定节点ID、IP和端口
            ClusterNode node = new ClusterNode(nodeId, ip, port, true);
            
            // 创建Redis服务
            MyRedisService service = new MyRedisService(port);

            // 初始化从节点列表
            if (node.getSlaves() == null) {
                node.addSlave(null);
                node.getSlaves().clear();
            }
            
            // 相互关联
            node.setService(service);
            service.setCluster(this);
            service.setCurrentNode(node);
            
            // 将节点和服务添加到集群中
            nodes.put(nodeId, node);
            services.put(nodeId, service);

        } catch (Exception e) {
            System.err.println("添加节点 " + nodeId + " 失败: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public void start() {
        for (Map.Entry<String, MyRedisService> entry : services.entrySet()) {
            try {
                System.out.println("启动节点: " + entry.getKey() + " 端口: " + entry.getValue().getPort());
                entry.getValue().start();
                System.out.println("节点 " + entry.getKey() + " 启动成功");
            } catch (Exception e) {
                System.err.println("启动节点 " + entry.getKey() + " 失败: " + e.getMessage());
            }
        }
    }

    public void connectNodes() {
        connectNodes(null);
    }

    public void connectNodes(CountDownLatch connectLatch) {
        // 检查是否启用分片
        if (shardingEnabled) {
            // 分片模式下使用全连接网络
            connectAllNodes(connectLatch);
        } else {
            // 非分片模式下，只从从节点连接到主节点
            connectSlavesToMasters(connectLatch);
        }
    }

    // 创建全连接网络 - 每个节点连接到所有其他节点
    private void connectAllNodes(CountDownLatch connectLatch) {
        // 计算需要建立的连接总数
        int totalConnections = nodes.size() * (nodes.size() - 1);
        System.out.println("开始建立集群全连接网络，需要建立 " + totalConnections + " 个连接...");

        final CountDownLatch effectiveConnectLatch;
        if (connectLatch != null && connectLatch.getCount() == totalConnections) {
            effectiveConnectLatch = connectLatch;
        } else {
            effectiveConnectLatch = new CountDownLatch(totalConnections);
        }

        // 为每个节点创建与其他节点的连接
        for (Map.Entry<String, ClusterNode> entry : nodes.entrySet()) {
            String currentNodeId = entry.getKey();
            ClusterNode currentNode = entry.getValue();

            // 确保当前节点的RedisCore已设置
            if (currentNode.getRedisCore() == null && currentNode.getService() != null) {
                currentNode.setRedisCore(currentNode.getService().getRedisCore());
            }

            // 连接其他所有节点
            for (Map.Entry<String, ClusterNode> otherEntry : nodes.entrySet()) {
                String otherNodeId = otherEntry.getKey();
                ClusterNode otherNode = otherEntry.getValue();
                
                // 不需要自己连接自己
                if (currentNodeId.equals(otherNodeId)) {
                    continue;
                }

                System.out.printf("节点 %s 开始连接到节点 %s (%s:%d)%n", 
                        currentNodeId, otherNodeId, otherNode.getIp(), otherNode.getPort());

                // 创建到目标节点的连接
                currentNode.connect().thenRun(() -> {
                    System.out.printf("节点 %s 成功连接到节点 %s%n", currentNodeId, otherNodeId);
                    
                    // 如果提供了CountDownLatch，减少计数
                    if (effectiveConnectLatch != null) {
                        effectiveConnectLatch.countDown();
                        long remaining = effectiveConnectLatch.getCount();
                        if (remaining % 5 == 0 || remaining < 5) {
                            System.out.printf("集群连接进度: 剩余 %d 个连接%n", remaining);
                        }
                    }
                }).exceptionally(e -> {
                    System.err.printf("连接失败: %s -> %s: %s%n",
                            currentNodeId, otherNodeId, e.getMessage());
                    
                    // 连接失败也需要减少计数
                    if (effectiveConnectLatch != null) {
                        effectiveConnectLatch.countDown();
                    }
                    return null;
                });
            }
        }

        // 等待所有连接完成
        try {
            boolean allConnected = effectiveConnectLatch.await(30, TimeUnit.SECONDS);
            if (allConnected) {
                System.out.println("集群全连接网络建立完成");
            } else {
                System.err.println("集群连接超时，部分连接可能未成功建立");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("等待集群连接时被中断");
        }
    }

    // 只从从节点连接到主节点
    private void connectSlavesToMasters(CountDownLatch connectLatch) {
        System.out.println("在主从模式下建立连接...");

        // 找出主节点和从节点
        List<ClusterNode> slaveNodes = new ArrayList<>();
        final ClusterNode masterNode;

        // 查找主节点和从节点
        ClusterNode tempMasterNode = null;
        for (ClusterNode node : nodes.values()) {
            if (node.isMaster() && node.getSlaves() != null && !node.getSlaves().isEmpty()) {
                tempMasterNode = node;
            } else if (!node.isMaster()) {
                slaveNodes.add(node);
            }
        }
        masterNode = tempMasterNode;

        if (masterNode == null) {
            System.err.println("错误：找不到主节点");
            return;
        }

        System.out.printf("找到主节点: %s (%s:%d)，从节点数量: %d%n", 
                masterNode.getId(), masterNode.getIp(), masterNode.getPort(), slaveNodes.size());

        // 确保主节点的RedisCore已设置
        if (masterNode.getRedisCore() == null && masterNode.getService() != null) {
            masterNode.setRedisCore(masterNode.getService().getRedisCore());
        }

        // 总连接数是双向的
        int totalConnections = slaveNodes.size() * 2;
        System.out.println("需要建立 " + totalConnections + " 个连接");
        

        final CountDownLatch effectiveConnectLatch;
        if (connectLatch != null && connectLatch.getCount() == totalConnections) {
            effectiveConnectLatch = connectLatch;
        } else {
            effectiveConnectLatch = new CountDownLatch(totalConnections);
        }

        // 为每个从节点建立双向连接
        for (ClusterNode slaveNode : slaveNodes) {
            final String slaveId = slaveNode.getId();
            final String masterId = masterNode.getId();

            System.out.printf("准备建立连接: 从节点 %s (%s:%d) <-> 主节点 %s (%s:%d)%n",
                    slaveId, slaveNode.getIp(), slaveNode.getPort(),
                    masterId, masterNode.getIp(), masterNode.getPort());

            // 确保从节点的RedisCore已设置
            if (slaveNode.getRedisCore() == null && slaveNode.getService() != null) {
                slaveNode.setRedisCore(slaveNode.getService().getRedisCore());
            }

            // 1. 从节点到主节点的连接
            System.out.printf("开始连接: 从节点 %s -> 主节点 %s%n", slaveId, masterId);
            slaveNode.connect().thenRun(() -> {
                System.out.printf("从节点 %s 成功连接到主节点 %s%n", slaveId, masterId);
                effectiveConnectLatch.countDown();
                
                // 2. 主节点到从节点的连接（用于复制命令）
                System.out.printf("开始连接: 主节点 %s -> 从节点 %s%n", masterId, slaveId);
                masterNode.connect().thenRun(() -> {
                    System.out.printf("主节点 %s 成功连接到从节点 %s%n", masterId, slaveId);
                    
                    // 将从节点添加到主节点的从节点列表（如果尚未添加）
                    if (!masterNode.getSlaves().contains(slaveNode)) {
                        masterNode.addSlave(slaveNode);
                        System.out.printf("将从节点 %s 添加到主节点 %s 的从节点列表%n", slaveId, masterId);
                    }
                    
                    effectiveConnectLatch.countDown();
                }).exceptionally(e -> {
                    System.err.printf("主节点连接从节点失败: %s -> %s: %s%n", masterId, slaveId, e.getMessage());
                    effectiveConnectLatch.countDown();
                    return null;
                });

            }).exceptionally(e -> {
                System.err.printf("从节点连接主节点失败: %s -> %s: %s%n", slaveId, masterId, e.getMessage());
                // 两次减少计数，因为主到从的连接也不会建立
                effectiveConnectLatch.countDown();
                effectiveConnectLatch.countDown();
                return null;
            });
        }
        
        // 等待所有连接完成
        try {
            boolean allConnected = effectiveConnectLatch.await(30, TimeUnit.SECONDS);
            if (allConnected) {
                System.out.println("主从节点连接建立完成");
            } else {
                System.err.println("主从连接超时，部分连接可能未成功建立");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("等待主从连接时被中断");
        }
    }

    // 寻找从节点对应的主节点
    private ClusterNode findMasterForSlave(ClusterNode slaveNode) {
        // 遍历所有主节点
        for (ClusterNode node : nodes.values()) {
            if (node.isMaster() && node.getSlaves() != null) {
                // 检查该主节点的从节点列表是否包含这个从节点
                for (ClusterNode slave : node.getSlaves()) {
                    if (slave != null && slave.getId().equals(slaveNode.getId())) {
                        return node;
                    }
                }
            }
        }
        return null;
    }

    // 检查主节点的从节点列表中是否包含特定从节点
    private boolean masterHasSlave(ClusterNode master, ClusterNode slave) {
        if (master.getSlaves() == null) {
            return false;
        }

        for (ClusterNode existingSlave : master.getSlaves()) {
            if (existingSlave != null && existingSlave.getId().equals(slave.getId())) {
                return true;
            }
        }

        return false;
    }

    @Override
    public String getNodeForKey(BytesWrapper key) {
        if (shardingEnabled && shardingStrategy != null) {
            return shardingStrategy.getNodeForKey(key);
        }
        // 如果分片未启用或策略未初始化，返回第一个可用节点
        return nodes.isEmpty() ? null : nodes.keySet().iterator().next();
    }

    public boolean isShardingEnabled() {
        return shardingEnabled;
    }

    public void setShardingEnabled(boolean shardingEnabled) {
        this.shardingEnabled = shardingEnabled;
    }

    /**
     * 获取集群中所有节点的ID列表
     *
     * @return 节点ID列表
     */
    public List<String> getNodeIds() {
        return new ArrayList<>(nodes.keySet());
    }

    /**
     * 根据节点ID获取ClusterNode对象
     *
     * @param nodeId 节点ID
     * @return ClusterNode对象，如果不存在返回null
     */
    public ClusterNode getClusterNode(String nodeId) {
        return nodes.get(nodeId);
    }

    /**
     * 向另一个节点发送消息
     * 
     * @param fromNodeId 发送消息的节点ID
     * @param toNodeId 接收消息的节点ID
     * @param message 要发送的消息
     * @return 如果发送成功返回true，否则返回false
     */
    public boolean sendMessage(String fromNodeId, String toNodeId, Resp message) {
        try {
            // 检查参数
            if (fromNodeId == null || toNodeId == null || message == null) {
                System.err.println("sendMessage参数错误: 发送方=" + fromNodeId 
                        + ", 接收方=" + toNodeId + ", 消息=" + (message != null));
                return false;
            }
            
            // 获取源节点和目标节点
            ClusterNode sourceNode = nodes.get(fromNodeId);
            ClusterNode targetNode = nodes.get(toNodeId);
            
            if (sourceNode == null) {
                System.err.println("发送方节点不存在: " + fromNodeId);
                return false;
            }
            
            if (targetNode == null) {
                System.err.println("接收方节点不存在: " + toNodeId);
                return false;
            }
            
            // 检查节点是否活跃
            if (!targetNode.isActive()) {
                System.err.println("接收方节点 " + toNodeId + " 连接不活跃，尝试重新连接");
                
                // 尝试重连
                try {
                    targetNode.connect().get(3, TimeUnit.SECONDS);
                } catch (Exception e) {
                    System.err.println("无法重新连接到节点 " + toNodeId + ": " + e.getMessage());
                    return false;
                }
                
                // 重新检查连接
                if (!targetNode.isActive()) {
                    System.err.println("重连失败，节点 " + toNodeId + " 仍然不可用");
                    return false;
                }
            }
            
            // 发送消息
            System.out.println("从节点 " + fromNodeId + " 向节点 " + toNodeId + " 发送消息: " + formatMessage(message));
            targetNode.sendMessage(message);
            return true;
        } catch (Exception e) {
            System.err.println("向节点 " + toNodeId + " 发送消息时出错: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    /**
     * 将RESP消息格式化为简短的文本描述
     */
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
                sb.append(((BulkString) respArray[0]).getContent().toUtf8String());
                
                if (respArray.length > 1) {
                    sb.append(" [");
                    sb.append(respArray.length - 1);
                    sb.append("个参数]");
                }
            } else {
                sb.append("非标准命令数组");
            }
            
            return sb.toString();
        } else if (message instanceof SimpleString) {
            return ((SimpleString) message).getContent();
        } else if (message instanceof BulkString) {
            BytesWrapper content = ((BulkString) message).getContent();
            return content != null ? content.toUtf8String() : "(nil)";
        } else {
            return message.toString();
        }
    }

    /**
     * 处理节点优雅下线
     * 确保当一个节点下线时不会影响其他节点的运行
     * 
     * @param nodeId 要下线的节点ID
     */
    @Override
    public void handleNodeGracefulShutdown(String nodeId) {
        ClusterNode node = nodes.get(nodeId);
        
        if (node == null) {
            System.err.println("无法处理节点下线: 未找到节点 " + nodeId);
            return;
        }
        
        System.out.println("处理节点 " + nodeId + " 优雅下线");
        
        try {
            // 锁定当前正在处理的节点，阻止其他操作同时进行
            synchronized (node) {
                boolean isMaster = node.isMaster();
                
                if (isMaster) {
                    System.out.println("主节点 " + nodeId + " 正在下线");
                    // 主节点下线，通知从节点但不执行转移操作
                    if (node.getSlaves() != null) {
                        for (ClusterNode slave : node.getSlaves()) {
                            if (slave != null && slave.isActive()) {
                                try {
                                    System.out.println("通知从节点 " + slave.getId() + " 主节点下线");
                                    // 这里只是通知，不触发转移逻辑
                                    RespArray infoCommand = new RespArray(new Resp[]{
                                        new BulkString(new BytesWrapper("_MASTER_OFFLINE".getBytes(BytesWrapper.CHARSET))),
                                        new BulkString(new BytesWrapper(nodeId.getBytes(BytesWrapper.CHARSET)))
                                    });
                                    slave.sendMessage(infoCommand);
                                } catch (Exception e) {
                                    System.err.println("通知从节点失败: " + e.getMessage());
                                }
                            }
                        }
                    }
                } else {
                    System.out.println("从节点 " + nodeId + " 正在下线");
                    // 从节点下线，通知主节点
                    String masterId = node.getMasterId();
                    if (masterId != null) {
                        ClusterNode master = nodes.get(masterId);
                        if (master != null && master.isActive()) {
                            try {
                                System.out.println("通知主节点 " + masterId + " 从节点下线");
                                RespArray infoCommand = new RespArray(new Resp[]{
                                    new BulkString(new BytesWrapper("_SLAVE_OFFLINE".getBytes(BytesWrapper.CHARSET))),
                                    new BulkString(new BytesWrapper(nodeId.getBytes(BytesWrapper.CHARSET)))
                                });
                                master.sendMessage(infoCommand);
                                
                                // 从主节点的从节点列表中移除
                                if (master.getSlaves() != null) {
                                    master.getSlaves().removeIf(slave -> slave != null && slave.getId().equals(nodeId));
                                    System.out.println("已从主节点的从节点列表中移除: " + nodeId);
                                }
                            } catch (Exception e) {
                                System.err.println("通知主节点失败: " + e.getMessage());
                            }
                        }
                    }
                }
                
                // 安全断开节点连接
                try {
                    node.disconnect();
                    System.out.println("已断开节点 " + nodeId + " 的连接");
                } catch (Exception e) {
                    System.err.println("断开节点连接失败: " + e.getMessage());
                }
                
                // 从集群服务列表中移除，但保留节点信息以便后续恢复
                services.remove(nodeId);
                System.out.println("节点 " + nodeId + " 已从活跃服务列表中移除");
                
                // 保留在nodes集合中，但标记为非活跃
                System.out.println("节点 " + nodeId + " 已完成优雅下线处理");
            }
        } catch (Exception e) {
            System.err.println("处理节点下线时发生错误: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
