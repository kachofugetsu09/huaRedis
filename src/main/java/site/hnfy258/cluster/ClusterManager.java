package site.hnfy258.cluster;

import site.hnfy258.protocal.Resp;
import site.hnfy258.server.MyRedisService;

import java.io.IOException;

public class ClusterManager {
    private RedisCluster cluster;

    public ClusterManager() {
        this.cluster = new RedisCluster();
    }

    public void initializeCluster() throws IOException, InterruptedException {
        // 先添加所有节点
        cluster.addNode("node1", "localhost", 6379);

        cluster.addNode("node2", "localhost", 6380);
        cluster.addNode("node3", "localhost", 6381);

        // 先启动所有节点服务
        cluster.start();

        // 等待所有节点启动完成
        Thread.sleep(2000); // 等待2秒确保服务启动

        // 然后建立节点间连接
        cluster.connectNodes();

        // 再等待连接建立
        Thread.sleep(1000);
    }

    public void sendMessageBetweenNodes(String fromNodeId, String toNodeId, Resp message) {
        MyRedisService fromService = cluster.getNode(fromNodeId);
        if (fromService != null) {
            fromService.sendMessageToNode(toNodeId, message);
        }
    }

    public void stopCluster() {
        cluster.stop();
    }
}
