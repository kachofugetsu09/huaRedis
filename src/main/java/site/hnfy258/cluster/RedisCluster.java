package site.hnfy258.cluster;

import site.hnfy258.RedisCoreImpl;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.SimpleString;
import site.hnfy258.server.MyRedisService;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

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
            System.out.println("Initializing sharding with nodes: " + nodeIds);
            this.shardingStrategy = new ConsistentHashSharding(nodeIds);
        }
    }

    @Override
    public void stop() {
        for (MyRedisService service : services.values()) {
            service.close();
        }
    }

    @Override
    public MyRedisService getNode(String nodeId) {
        return services.get(nodeId);
    }

    public void addNode(String nodeId, String ip, int port) throws IOException {
        try {


            ClusterNode node = new ClusterNode(nodeId, ip, port, true);
            MyRedisService service = new MyRedisService(port);


            if (node.getSlaves() == null) {
                node.addSlave(null);
                node.getSlaves().clear();
            }


            nodes.put(nodeId, node);


            service.setCluster(this);
            node.setService(service);

            service.setCurrentNode(node);

            services.put(nodeId, service);

        } catch (Exception e) {
            System.err.println("Failed to add node " + nodeId + ": " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public void start() {
        for (Map.Entry<String, MyRedisService> entry : services.entrySet()) {
            try {
                System.out.println("Starting node: " + entry.getKey() + " on port " + entry.getValue().getPort());
                entry.getValue().start();
                System.out.println("Node " + entry.getKey() + " started successfully");
            } catch (Exception e) {
                System.err.println("Failed to start node " + entry.getKey() + ": " + e.getMessage());
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

        // 为每个节点创建与其他节点的连接
        for (Map.Entry<String, MyRedisService> entry : services.entrySet()) {
            String currentNodeId = entry.getKey();
            MyRedisService currentService = entry.getValue();

            // 连接其他所有节点
            for (Map.Entry<String, ClusterNode> otherEntry : nodes.entrySet()) {
                String otherNodeId = otherEntry.getKey();
                if (!currentNodeId.equals(otherNodeId)) {
                    ClusterNode otherNode = otherEntry.getValue();

                    // 创建客户端连接
                    ClusterClient client = new ClusterClient(otherNode.getIp(), otherNode.getPort(), currentService.getRedisCore());
                    client.connect().thenRun(() -> {
                        System.out.printf("Node %s successfully connected to node %s%n", currentNodeId, otherNodeId);
                        // 将client保存到当前服务的clusterClients中
                        currentService.addClusterClient(otherNodeId, client);
                        // 如果提供了CountDownLatch，减少计数
                        if (connectLatch != null) {
                            connectLatch.countDown();
                        }
                    }).exceptionally(e -> {
                        System.err.printf("Connection failed: %s -> %s: %s%n",
                                currentNodeId, otherNodeId, e.getMessage());
                        // 连接失败也需要减少计数
                        if (connectLatch != null) {
                            connectLatch.countDown();
                        }
                        return null;
                    });
                }
            }
        }
    }

    // 只从从节点连接到主节点 - 符合Redis原生主从架构
    private void connectSlavesToMasters(CountDownLatch connectLatch) {
        System.out.println("在主从模式下建立双向连接...");

        // 找出主节点和从节点
        List<ClusterNode> slaveNodes = new ArrayList<>();
        ClusterNode masterNode = null;

        for (ClusterNode node : nodes.values()) {
            if (node.isMaster() && node.getSlaves() != null && !node.getSlaves().isEmpty()) {
                masterNode = node;
            } else if (!node.isMaster()) {
                slaveNodes.add(node);
            }
        }

        if (masterNode == null || masterNode.getService() == null) {
            System.err.println("错误：找不到主节点或主节点服务实例为空");
            return;
        }

        // 总连接数是双向的
        int totalConnections = slaveNodes.size() * 2;
        // 如果提供了CountDownLatch，使用它，否则创建新的
        final CountDownLatch effectiveConnectLatch = connectLatch != null ?
                connectLatch : new CountDownLatch(totalConnections);

        MyRedisService masterService = masterNode.getService();

        // 为每个从节点建立双向连接
        for (ClusterNode slaveNode : slaveNodes) {
            if (slaveNode.getService() == null) continue;

            final String slaveId = slaveNode.getId();
            final String masterId = masterNode.getId();
            MyRedisService slaveService = slaveNode.getService();

            // 1. 从节点到主节点的连接
            ClusterClient slave2master = new ClusterClient(masterNode.getIp(), masterNode.getPort(), slaveService.getRedisCore());

            slave2master.connect().thenRun(() -> {
                slaveService.addClusterClient(masterId, slave2master);
                effectiveConnectLatch.countDown();

                // 2. 主节点到从节点的连接（用于复制命令）
                ClusterClient master2slave = new ClusterClient(slaveNode.getIp(), slaveNode.getPort(), masterService.getRedisCore());

                master2slave.connect().thenRun(() -> {
                    masterService.addClusterClient(slaveId, master2slave);
                    effectiveConnectLatch.countDown();
                }).exceptionally(e -> {
                    System.err.println("主节点连接从节点失败: " + masterId + " -> " + slaveId);
                    effectiveConnectLatch.countDown();
                    return null;
                });

            }).exceptionally(e -> {
                System.err.println("从节点连接主节点失败: " + slaveId + " -> " + masterId);
                // 两次减少计数，因为主到从的连接也不会建立
                effectiveConnectLatch.countDown();
                effectiveConnectLatch.countDown();
                return null;
            });
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
}
