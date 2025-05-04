package site.hnfy258.command.impl;

import org.apache.log4j.Logger;
import site.hnfy258.RedisCore;
import site.hnfy258.cluster.ClusterNode;
import site.hnfy258.cluster.RedisCluster;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Errors;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.SimpleString;
import site.hnfy258.server.MyRedisService;

/**
 * 实现SLAVEOF命令，用于更改节点的主从角色
 *
 * 用法：
 * 1. SLAVEOF NO ONE - 将从节点提升为主节点
 * 2. SLAVEOF host port - 将节点设置为指定主节点的从节点,未实现
 */
public class SlaveOf implements Command {
    private static final Logger logger = Logger.getLogger(SlaveOf.class);

    private final RedisCore redisCore;
    private String host;
    private int port;
    private boolean promoteToMaster = false;

    public SlaveOf(RedisCore redisCore) {
        this.redisCore = redisCore;
    }

    @Override
    public CommandType getType() {
        return CommandType.SLAVEOF;
    }

    @Override
    public void setContext(Resp[] array) {
        if (array.length < 3) {
            throw new IllegalArgumentException("SLAVEOF command requires at least two arguments");
        }

        String arg1 = ((BulkString) array[1]).getContent().toUtf8String().toUpperCase();
        String arg2 = ((BulkString) array[2]).getContent().toUtf8String().toUpperCase();

        // 检查是否是 "SLAVEOF NO ONE" 命令
        if ("NO".equals(arg1) && "ONE".equals(arg2)) {
            promoteToMaster = true;
        } else {
            // 解析主节点的主机和端口（暂不实现）
            host = ((BulkString) array[1]).getContent().toUtf8String();
            try {
                port = Integer.parseInt(((BulkString) array[2]).getContent().toUtf8String());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid port number: " + arg2);
            }
        }
    }

    @Override
    public Resp handle() {
        MyRedisService service = redisCore.getRedisService();
        ClusterNode currentNode = service.getCurrentNode();

        if (currentNode == null) {
            return new Errors("ERR node not initialized");
        }

        // 处理提升为主节点的情况
        if (promoteToMaster) {
            if (currentNode.isMaster()) {
                return new SimpleString("OK (already master)");
            }

            try {
                // 获取集群对象
                RedisCluster cluster = service.getCluster();
                if (cluster == null) {
                    return new Errors("ERR cluster not available");
                }

                logger.info("开始将节点 " + currentNode.getId() + " 从从节点提升为主节点");

                // 1. 找到当前节点的主节点，并从其从节点列表中移除当前节点
                ClusterNode masterNode = findMasterNode(cluster, currentNode);
                if (masterNode != null) {
                    logger.info("从原主节点 " + masterNode.getId() + " 的从节点列表中移除节点 " + currentNode.getId());
                    masterNode.removeSlave(currentNode);
                }

                // 2. 更新节点角色
                currentNode.setMaster(true);

                // 3. 初始化复制处理器
                service.setCurrentNode(currentNode); // 这会重新初始化复制处理器

                logger.info("节点 " + currentNode.getId() + " 已成功提升为主节点");

                return new SimpleString("OK");
            } catch (Exception e) {
                logger.error("提升节点为主节点失败: " + e.getMessage(), e);
                return new Errors("ERR failed to promote node to master: " + e.getMessage());
            }
        } else {
            // 暂不实现将节点设置为从节点的功能
            return new Errors("ERR SLAVEOF with host and port is not implemented yet");
        }
    }

    /**
     * 在集群中查找当前节点的主节点
     *
     * @param cluster 集群对象
     * @param slaveNode 从节点
     * @return 主节点，如果找不到返回null
     */
    private ClusterNode findMasterNode(RedisCluster cluster, ClusterNode slaveNode) {
        // 遍历集群中的所有节点
        for (String nodeId : cluster.getNodeIds()) {
            ClusterNode node = cluster.getClusterNode(nodeId);

            // 检查该节点是否是主节点且有从节点列表
            if (node != null && node.isMaster() && node.getSlaves() != null) {
                // 检查从节点列表中是否包含当前节点
                for (ClusterNode slave : node.getSlaves()) {
                    if (slave != null && slave.getId().equals(slaveNode.getId())) {
                        return node; // 找到了主节点
                    }
                }
            }
        }

        return null; // 找不到主节点
    }
}
