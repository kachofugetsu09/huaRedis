package site.hnfy258;

import site.hnfy258.cluster.ClusterNode;
import site.hnfy258.cluster.RedisCluster;
import site.hnfy258.sentinel.Sentinel;
import site.hnfy258.server.MyRedisService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class RedisClusterLauncher {
    private static final int[] PORTS = {6379, 6380, 6381, 6382};
    private static final int[] SENTINEL_PORTS = {26379, 26380, 26381};
    
    // 全局设置：是否启用分片模式（设为true启用分片，设为false使用主从模式）
    private static final boolean USE_SHARDING_MODE = false;
    
    // 存储所有哨兵实例
    private static final List<Sentinel> sentinels = new ArrayList<>();
    
    // 存储集群对象
    private static RedisCluster cluster;

    public static void main(String[] args) {
        System.out.println("==== 启动Redis" + (USE_SHARDING_MODE ? "分片" : "主从") + "集群 ====");
        
        // 创建集群对象，指定是否启用分片
        cluster = new RedisCluster(USE_SHARDING_MODE);
        CountDownLatch latch = new CountDownLatch(PORTS.length);

        for (int i = 0; i < PORTS.length; i++) {
            final String nodeId = "node" + (i + 1);
            final int port = PORTS[i];
            new Thread(() -> startNode(cluster, nodeId, port, latch)).start();
        }

        try {
            latch.await(); // 等待所有节点启动
            System.out.println("所有节点启动完成");

            if (USE_SHARDING_MODE) {
                // 分片模式设置
                setupShardingMode(cluster);
            } else {
                // 主从模式设置
                setupMasterSlaveMode(cluster);
                
                // 启动哨兵节点
                startSentinels();
            }
            
            // 保持程序运行
            keepRunning();
        } catch (InterruptedException e) {
            System.err.println("集群启动被中断: " + e.getMessage());
        }
    }
    
    private static void startSentinels() {
        System.out.println("[哨兵] 启动哨兵节点...");
        
        // 准备主节点和从节点列表
        ClusterNode masterNode = cluster.getClusterNode("node1");
        List<ClusterNode> slaveNodes = new ArrayList<>();
        
        for (int i = 1; i < PORTS.length; i++) {
            String slaveId = "node" + (i + 1);
            ClusterNode slaveNode = cluster.getClusterNode(slaveId);
            if (slaveNode != null) {
                slaveNodes.add(slaveNode);
            }
        }
        
        // 创建并启动三个哨兵节点
        for (int i = 0; i < SENTINEL_PORTS.length; i++) {
            final String sentinelId = "sentinel" + (i + 1);
            final int port = SENTINEL_PORTS[i];
            
            try {
                // 创建哨兵实例 (quorum=2，表示需要2个哨兵同意才能判定主节点下线)
                Sentinel sentinel = new Sentinel(sentinelId, "localhost", port, 2);
                
                // 启动哨兵
                if (sentinel.start()) {
                    System.out.println("[哨兵] " + sentinelId + " 启动成功，监听端口: " + port);
                    sentinels.add(sentinel);
                    
                    // 监控整个集群（主节点和所有从节点）
                    sentinel.monitorCluster( masterNode, slaveNodes);
                    
                    // 如果不是第一个哨兵，则添加前面的哨兵作为邻居
                    if (i > 0) {
                        for (int j = 0; j < i; j++) {
                            sentinel.addSentinelNeighbor("sentinel" + (j + 1), "localhost", SENTINEL_PORTS[j]);
                        }
                    }
                } else {
                    System.err.println("[哨兵] " + sentinelId + " 启动失败");
                }
            } catch (Exception e) {
                System.err.println("[哨兵] " + sentinelId + " 启动异常: " + e.getMessage());
            }
        }
        
        System.out.println("[哨兵] 哨兵节点启动完成，监控 1 个主节点和 " + slaveNodes.size() + " 个从节点");
    }
    
    private static void setupShardingMode(RedisCluster cluster) {
        System.out.println("[分片模式] 初始化分片策略...");
        
        // 初始化分片策略
        cluster.initializeSharding();
        
        // 计算全连接网络的连接数
        int nodeCount = PORTS.length;
        int totalConnections = nodeCount * (nodeCount - 1);
        
        // 设置连接计数器
        CountDownLatch connectLatch = new CountDownLatch(totalConnections);
        
        // 建立节点之间的连接
        cluster.connectNodes(connectLatch);
        
        try {
            // 等待连接完成
            boolean allConnected = connectLatch.await(10, java.util.concurrent.TimeUnit.SECONDS);
            if (!allConnected) {
                System.err.println("[警告] 部分节点连接超时");
            }
            
            System.out.println("[完成] 分片模式配置就绪");
        } catch (InterruptedException e) {
            System.err.println("节点连接被中断: " + e.getMessage());
        }
    }

    private static void setupMasterSlaveMode(RedisCluster cluster) {
        System.out.println("[主从模式] 设置主从关系...");
        
        // 配置主节点
        MyRedisService masterService = cluster.getNode("node1");
        ClusterNode master = masterService.getCurrentNode();
        master.setMaster(true);
        
        // 获取所有从节点服务和节点对象
        MyRedisService[] slaveServices = new MyRedisService[PORTS.length - 1];
        for (int i = 0; i < slaveServices.length; i++) {
            slaveServices[i] = cluster.getNode("node" + (i + 2));
        }
        
        ClusterNode[] slaveNodes = new ClusterNode[slaveServices.length];
        for (int i = 0; i < slaveServices.length; i++) {
            if (slaveServices[i] != null) {
                slaveNodes[i] = slaveServices[i].getCurrentNode();
                slaveNodes[i].setMaster(false); // 设置为从节点
                master.addSlave(slaveNodes[i]); // 添加到主节点的从节点列表
            }
        }

        // 使用CountDownLatch来等待所有从节点连接完成
        int slaveCount = slaveNodes.length;
        CountDownLatch connectLatch = new CountDownLatch(slaveCount * 2); // 每个从节点建立双向连接
        
        // 建立主从节点之间的连接
        cluster.connectNodes(connectLatch);
        
        try {
            // 等待连接完成
            boolean allConnected = connectLatch.await(10, java.util.concurrent.TimeUnit.SECONDS);
            if (!allConnected) {
                System.err.println("[警告] 部分连接超时");
            }
            
            // 连接完成后重新初始化复制处理器
            masterService.setCurrentNode(master);
            
            System.out.println("[完成] 主从复制配置就绪");
        } catch (InterruptedException e) {
            System.err.println("主从连接过程被中断: " + e.getMessage());
        }
    }

    private static void startNode(RedisCluster cluster, String nodeId, int port, CountDownLatch latch) {
        try {
            System.out.println("[节点] 启动 " + nodeId + " (端口:" + port + ")");
            cluster.addNode(nodeId, "localhost", port);
            MyRedisService service = cluster.getNode(nodeId);
            service.start();
            System.out.println("[节点] " + nodeId + " 启动成功");
        } catch (IOException e) {
            System.err.println("[错误] 节点 " + nodeId + " 启动失败: " + e.getMessage());
        } finally {
            latch.countDown();
        }
    }

    private static void keepRunning() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[系统] 正在关闭Redis集群和哨兵...");
            
            // 关闭所有哨兵
            for (Sentinel sentinel : sentinels) {
                sentinel.shutdown();
            }
        }));
        
        // 打印使用提示
        System.out.println("\n==== Redis集群已成功启动 ====");
        System.out.println("按Ctrl+C停止服务");
        
        // 等待程序被中断
        try {
            Thread.currentThread().join();
        } catch (InterruptedException ignored) {
        }
    }
}
