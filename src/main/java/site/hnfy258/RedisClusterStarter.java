package site.hnfy258;

import site.hnfy258.cluster.ClusterManager;
import site.hnfy258.protocal.SimpleString;

import java.io.IOException;

public class RedisClusterStarter {
    public static void main(String[] args) throws IOException, InterruptedException {
        ClusterManager clusterManager = new ClusterManager();
        clusterManager.initializeCluster();

        // 模拟节点间通信
        clusterManager.sendMessageBetweenNodes("node1", "node2", new SimpleString("Hello from node1"));
        clusterManager.sendMessageBetweenNodes("node2", "node3", new SimpleString("Hello from node2"));

        // 运行一段时间后关闭集群
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        clusterManager.stopCluster();
    }
}
