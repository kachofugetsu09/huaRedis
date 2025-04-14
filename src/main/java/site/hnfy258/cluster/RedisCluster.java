package site.hnfy258.cluster;

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

public class RedisCluster implements Cluster {
    private Map<String, ClusterNode> nodes;
    private Map<String, MyRedisService> services;
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
            System.out.println("Attempting to add node: " + nodeId + " on " + ip + ":" + port);
            ClusterNode node = new ClusterNode(nodeId, ip, port, true);
            nodes.put(nodeId, node);
            MyRedisService service = new MyRedisService(port);
            service.setCluster(this);
            service.setCurrentNode(node);
            services.put(nodeId, service);
            System.out.println("Successfully added node: " + nodeId);
        } catch (Exception e) {
            System.err.println("Failed to add node " + nodeId + ": " + e.getMessage());
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
        // 为每个节点创建与其他节点的连接
        for (Map.Entry<String, MyRedisService> entry : services.entrySet()) {
            String currentNodeId = entry.getKey();
            MyRedisService currentService = entry.getValue();
            ClusterNode currentNode = nodes.get(currentNodeId);

            // 连接其他所有节点
            for (Map.Entry<String, ClusterNode> otherEntry : nodes.entrySet()) {
                String otherNodeId = otherEntry.getKey();
                if (!currentNodeId.equals(otherNodeId)) {
                    ClusterNode otherNode = otherEntry.getValue();

                    // 创建客户端连接
                    ClusterClient client = new ClusterClient(otherNode.getIp(), otherNode.getPort());
                    client.connect().thenRun(() -> {
                        System.out.printf("Node %s successfully connected to node %s%n", currentNodeId, otherNodeId);
                        // 将client保存到当前服务的clusterClients中
                        currentService.addClusterClient(otherNodeId, client);
                    }).exceptionally(e -> {
                        System.err.printf("Connection failed: %s -> %s: %s%n",
                                currentNodeId, otherNodeId, e.getMessage());
                        return null;
                    });
                }
            }
        }
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
}
