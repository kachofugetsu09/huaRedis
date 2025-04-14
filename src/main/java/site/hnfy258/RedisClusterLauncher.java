package site.hnfy258;

import site.hnfy258.cluster.RedisCluster;
import site.hnfy258.server.MyRedisService;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class RedisClusterLauncher {
    private static final int[] PORTS = {6379, 6380, 6381, 6382};
    private static final CountDownLatch LATCH = new CountDownLatch(PORTS.length);

    public static void main(String[] args) {
        RedisCluster cluster = new RedisCluster(true);

        for (int i = 0; i < PORTS.length; i++) {
            final String nodeId = "node" + (i + 1);
            final int port = PORTS[i];
            new Thread(() -> startNode(cluster, nodeId, port)).start();
        }

        try {
            LATCH.await(); // 等待所有节点启动
            System.out.println("All nodes started. Cluster is ready.");
            
            // 初始化分片
            cluster.initializeSharding();
            System.out.println("Sharding initialized.");

            // 保持程序运行
            keepRunning();
        } catch (InterruptedException e) {
            System.err.println("Cluster startup interrupted: " + e.getMessage());
        }
    }

    private static void startNode(RedisCluster cluster, String nodeId, int port) {
        try {
            System.out.println("Starting node " + nodeId + " on port " + port);
            cluster.addNode(nodeId, "localhost", port);
            MyRedisService service = cluster.getNode(nodeId);
            service.start();
            System.out.println("Node " + nodeId + " started successfully");
        } catch (IOException e) {
            System.err.println("Failed to start node " + nodeId + ": " + e.getMessage());
        } finally {
            LATCH.countDown();
        }
    }

    private static void keepRunning() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down Redis cluster...");
        }));

        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }
}
