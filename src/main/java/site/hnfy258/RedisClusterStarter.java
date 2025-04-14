package site.hnfy258;

import site.hnfy258.cluster.ClusterManager;
import site.hnfy258.command.CommandType;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;
import site.hnfy258.protocal.SimpleString;
import site.hnfy258.server.MyRedisService;

import java.io.IOException;
import java.util.Random;

public class RedisClusterStarter {
    public static void main(String[] args) throws IOException, InterruptedException {
        ClusterManager clusterManager = new ClusterManager();
        clusterManager.initializeCluster();

        // 显式初始化分片
        clusterManager.getCluster().initializeSharding();

        System.out.println("集群已初始化，开始测试分片功能...");

        // 测试基本通信
        clusterManager.sendMessageBetweenNodes("node1", "node2", new SimpleString("Hello from node1"));
        clusterManager.sendMessageBetweenNodes("node2", "node3", new SimpleString("Hello from node2"));

        // 等待通信完成
        Thread.sleep(1000);

        // 测试分片功能
        testSharding(clusterManager);

        // 运行一段时间后关闭集群
        System.out.println("测试完成，5秒后关闭集群...");
        Thread.sleep(5000);

        clusterManager.stopCluster();
    }

    private static void testSharding(ClusterManager clusterManager) throws InterruptedException {
        MyRedisService node1 = clusterManager.getCluster().getNode("node1");
        MyRedisService node2 = clusterManager.getCluster().getNode("node2");
        MyRedisService node3 = clusterManager.getCluster().getNode("node3");

        // 生成更分散的测试键
        String[] testKeys = new String[] {
                "user:1001", "product:2002", "order:3003",
                "cache:4004", "session:5005", "config:6006",
                "token:7007", "cart:8008", "log:9009", "event:1010"
        };

        // 打印分片信息
        for (String key : testKeys) {
            BytesWrapper keyBytes = new BytesWrapper(key.getBytes());
            String targetNodeId = clusterManager.getCluster().getNodeForKey(keyBytes);
            System.out.printf("Key '%s' -> Node %s%n", key, targetNodeId);
        }

        // 执行SET和GET
        for (String key : testKeys) {
            RespArray setCommand = createSetCommand(key, "value_for_" + key);
            node1.executeCommand(setCommand);
        }

        for (String key : testKeys) {
            RespArray getCommand = createGetCommand(key);
            String targetNodeId = clusterManager.getCluster().getNodeForKey(new BytesWrapper(key.getBytes()));
            MyRedisService targetNode = clusterManager.getCluster().getNode(targetNodeId);
            System.out.printf("GET '%s' from Node %s: ", key, targetNodeId);
            printResult(targetNode.executeCommand(getCommand));
        }
    }

    private static RespArray createSetCommand(String key, String value) {
        Resp[] array = new Resp[3];
        array[0] = new BulkString(new BytesWrapper("SET".getBytes()));
        array[1] = new BulkString(new BytesWrapper(key.getBytes()));
        array[2] = new BulkString(new BytesWrapper(value.getBytes()));
        return new RespArray(array);
    }

    private static RespArray createGetCommand(String key) {
        Resp[] array = new Resp[2];
        array[0] = new BulkString(new BytesWrapper("GET".getBytes()));
        array[1] = new BulkString(new BytesWrapper(key.getBytes()));
        return new RespArray(array);
    }

    private static void printResult(Object result) {
        if (result == null) {
            System.out.println("null");
        } else if (result instanceof BulkString) {
            BulkString bs = (BulkString) result;
            if (bs.isNull()) {
                System.out.println("(nil)");
            } else {
                System.out.println("\"" + bs.getContent().toUtf8String() + "\"");
            }
        } else if (result instanceof SimpleString) {
            System.out.println(((SimpleString) result).getContent());
        } else {
            System.out.println(result.toString());
        }
    }
}
