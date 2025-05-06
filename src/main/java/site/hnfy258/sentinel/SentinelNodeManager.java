package site.hnfy258.sentinel;

import io.netty.channel.Channel;
import org.apache.log4j.Logger;
import site.hnfy258.cluster.ClusterNode;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;
import site.hnfy258.protocal.SimpleString;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Sentinel节点管理器，负责管理和监控Redis节点
 */
public class SentinelNodeManager {
    private final Logger logger = Logger.getLogger(SentinelNodeManager.class);
    
    // 被监控的节点
    private final Map<String, ClusterNode> monitoredNodes = new ConcurrentHashMap<>();
    
    // 主观下线节点集合
    private final Set<String> subjectiveDownNodes = ConcurrentHashMap.newKeySet();
    
    // 客观下线节点集合
    private final Set<String> objectiveDownNodes = ConcurrentHashMap.newKeySet();
    
    // 节点最后回复时间
    private final Map<String, Long> lastReplyTime = new ConcurrentHashMap<>();

    /**
     * 添加要监控的主节点
     * @param masterId 主节点ID
     * @param ip 主节点IP
     * @param port 主节点端口
     * @return 操作是否成功
     */
    public boolean monitorMaster(String masterId, String ip, int port, Consumer<Resp> responseCallback) {
        // 检查是否已经在监控列表中
        if (monitoredNodes.containsKey(masterId)) {
            ClusterNode existingNode = monitoredNodes.get(masterId);
            // 如果连接断开，尝试重新连接
            if (!existingNode.isActive()) {
                logger.info("尝试重新连接到已存在的节点: " + masterId);
                existingNode.connect();
            }
            
            // 检查回调是否设置
            if (existingNode.getResponseCallback() == null) {
                logger.info("[DEBUG] 已存在节点 " + masterId + " 没有设置回调，重新设置回调");
                existingNode.setResponseCallback(responseCallback);
            } else {
                logger.info("[DEBUG] 已存在节点 " + masterId + " 已设置回调");
            }
            
            return true;
        }
        
        try {
            logger.info("Sentinel正在连接到节点: " + masterId + " (" + ip + ":" + port + ")");
            
            // 创建新的节点实例
            ClusterNode masterNode = new ClusterNode(masterId, ip, port, true);
            
            // 设置节点响应回调
            logger.info("[DEBUG] 为新节点 " + masterId + " 设置响应回调");
            masterNode.setResponseCallback(responseCallback);
            
            // 连接到主节点
            CompletableFuture<Void> connectFuture = masterNode.connect();
            connectFuture.get(5, TimeUnit.SECONDS);
            
            // 如果连接成功，添加到监控列表
            if (masterNode.isActive()) {
                monitoredNodes.put(masterId, masterNode);
                lastReplyTime.put(masterId, System.currentTimeMillis());
                logger.info("Sentinel成功连接到节点: " + masterId + "，初始化最后回复时间: " + lastReplyTime.get(masterId));
                return true;
            } else {
                logger.error("无法连接到节点: " + masterId);
                return false;
            }
        } catch (Exception e) {
            logger.error("监控主节点失败: " + masterId, e);
            return false;
        }
    }

    /**
     * 添加要监控的从节点
     * @param slaveId 从节点ID
     * @param ip 从节点IP
     * @param port 从节点端口
     * @return 操作是否成功
     */
    public boolean monitorSlave(String slaveId, String ip, int port, Consumer<Resp> responseCallback) {
        // 检查是否已经在监控列表中
        if (monitoredNodes.containsKey(slaveId)) {
            ClusterNode existingNode = monitoredNodes.get(slaveId);
            // 如果连接断开，尝试重新连接
            if (!existingNode.isActive()) {
                logger.info("尝试重新连接到已存在的从节点: " + slaveId);
                existingNode.connect();
            }
            
            // 检查回调是否设置
            if (existingNode.getResponseCallback() == null) {
                logger.info("已存在从节点 " + slaveId + " 没有设置回调，重新设置回调");
                existingNode.setResponseCallback(responseCallback);
            }
            
            return true;
        }
        
        try {
            logger.info("Sentinel正在连接到从节点: " + slaveId + " (" + ip + ":" + port + ")");
            
            // 创建新的节点实例（标记为非主节点）
            ClusterNode slaveNode = new ClusterNode(slaveId, ip, port, false);
            
            // 设置节点响应回调
            logger.info("为新从节点 " + slaveId + " 设置响应回调");
            slaveNode.setResponseCallback(responseCallback);
            
            // 连接到从节点
            CompletableFuture<Void> connectFuture = slaveNode.connect();
            connectFuture.get(5, TimeUnit.SECONDS);
            
            // 如果连接成功，添加到监控列表
            if (slaveNode.isActive()) {
                monitoredNodes.put(slaveId, slaveNode);
                lastReplyTime.put(slaveId, System.currentTimeMillis());
                logger.info("Sentinel成功连接到从节点: " + slaveId + "，初始化最后回复时间: " + lastReplyTime.get(slaveId));
                return true;
            } else {
                logger.error("无法连接到从节点: " + slaveId);
                return false;
            }
        } catch (Exception e) {
            logger.error("监控从节点失败: " + slaveId, e);
            return false;
        }
    }

    /**
     * 添加要监控的Redis节点（支持同时监控主节点和从节点）
     * @param masterNode 主节点
     * @param slaveNodes 从节点列表
     * @return 操作是否成功
     */
    public boolean monitorCluster(ClusterNode masterNode, List<ClusterNode> slaveNodes, Consumer<Resp> responseCallback) {
        boolean success = true;
        
        // 监控主节点
        if (masterNode != null) {
            success &= monitorMaster(masterNode.getId(), masterNode.getIp(), masterNode.getPort(), responseCallback);
            logger.info("监控主节点: " + masterNode.getId());
        }
        
        // 监控所有从节点
        if (slaveNodes != null) {
            for (ClusterNode slave : slaveNodes) {
                if (slave != null) {
                    success &= monitorSlave(slave.getId(), slave.getIp(), slave.getPort(), responseCallback);
                    logger.info("监控从节点: " + slave.getId());
                }
            }
        }
        
        return success;
    }

    /**
     * 向所有连接的节点发送PING命令
     */
    public void pingAllNodes() {
        // PING命令
        RespArray pingCommand = new RespArray(new Resp[]{
                new BulkString(new BytesWrapper("PING".getBytes(BytesWrapper.CHARSET)))
        });

        // 只PING未下线的节点
        for (Map.Entry<String, ClusterNode> entry : monitoredNodes.entrySet()) {
            String nodeId = entry.getKey();
            ClusterNode node = entry.getValue();
            
            // 跳过已经标记为下线的节点
            if (subjectiveDownNodes.contains(nodeId) || objectiveDownNodes.contains(nodeId)) {
                continue;
            }
            
            if (node.isActive()) {
                try {
                    node.sendMessage(pingCommand);
                    if (logger.isDebugEnabled()) {
                        logger.debug("已向节点 " + nodeId + " 发送PING");
                    }
                } catch (Exception e) {
                    logger.error("PING节点失败: " + nodeId, e);
                }
            }
        }
    }

    /**
     * 检查主节点是否被主观判定为下线
     * @param masterId 主节点ID
     * @return 是否下线
     */
    public boolean isMasterSubjectivelyDown(String masterId) {
        return subjectiveDownNodes.contains(masterId);
    }

    /**
     * 更新节点的最后回复时间
     * @param nodeId 节点ID
     */
    public void updateNodeLastReplyTime(String nodeId) {
        long currentTime = System.currentTimeMillis();
        lastReplyTime.put(nodeId, currentTime);
    }

    /**
     * 检查节点状态
     * @return 是否有新的主观下线节点
     */
    public boolean checkNodeStatus() {
        boolean hasNewSubjectiveDown = false;

        for (Map.Entry<String, ClusterNode> entry : monitoredNodes.entrySet()) {
            String nodeId = entry.getKey();
            ClusterNode node = entry.getValue();
            
            // 获取最后一次回复时间
            Long lastReply = lastReplyTime.get(nodeId);
            long currentTime = System.currentTimeMillis();
            
            // 如果节点连接已断开，尝试重新连接
            if (!node.isActive()) {
                try {
                    node.connect().get(1, TimeUnit.SECONDS);
                    
                    if (node.isActive()) {
                        logger.info("成功重新连接到节点: " + nodeId);
                        
                        long newTime = System.currentTimeMillis();
                        lastReplyTime.put(nodeId, newTime);
                        
                        if (subjectiveDownNodes.contains(nodeId)) {
                            logger.info("节点 " + nodeId + " 恢复上线，从主观下线列表中移除");
                            subjectiveDownNodes.remove(nodeId);
                        }
                        
                        continue;
                    }
                } catch (Exception e) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("重新连接到节点失败: " + nodeId);
                    }
                }
            }
            
            // 如果超过5秒没有回复，认为节点主观下线
            if (lastReply == null || (currentTime - lastReply) > 5000) {
                if (!subjectiveDownNodes.contains(nodeId)) {
                    // 将节点添加到主观下线列表
                    subjectiveDownNodes.add(nodeId);
                    logger.info("节点 " + nodeId + " 已主观下线，最后回复时间=" + lastReply + 
                                ", 超时: " + (lastReply == null ? "null" : (currentTime - lastReply) + "ms"));
                    hasNewSubjectiveDown = true;
                }
            } else {
                // 如果节点回复正常
                if (subjectiveDownNodes.contains(nodeId)) {
                    logger.info("节点 " + nodeId + " 恢复正常，从主观下线列表中移除");
                    subjectiveDownNodes.remove(nodeId);
                }
            }
        }
        
        return hasNewSubjectiveDown;
    }

    /**
     * 处理节点响应
     * @param nodeId 节点ID
     * @param resp 响应
     */
    public void handleNodeResponse(String nodeId, Resp resp) {
        if (resp instanceof SimpleString) {
            String content = ((SimpleString) resp).getContent();
            if ("PONG".equalsIgnoreCase(content)) {
                // 如果节点已经被标记为下线，但现在收到了PONG，需要重新评估其状态
                boolean wasOffline = subjectiveDownNodes.contains(nodeId) || objectiveDownNodes.contains(nodeId);
                
                // 更新节点最后回复时间
                updateNodeLastReplyTime(nodeId);
                
                // 如果之前被标记为下线状态，但现在收到了PONG，移除下线标记
                if (wasOffline) {
                    logger.info("曾被标记为下线的节点 " + nodeId + " 收到PONG响应，正在恢复其状态");
                    subjectiveDownNodes.remove(nodeId);
                    objectiveDownNodes.remove(nodeId);
                }
            }
        }
    }

    /**
     * 获取主观下线节点集合
     */
    public Set<String> getSubjectiveDownNodes() {
        return subjectiveDownNodes;
    }

    /**
     * 获取客观下线节点集合
     */
    public Set<String> getObjectiveDownNodes() {
        return objectiveDownNodes;
    }

    /**
     * 获取监控的节点
     */
    public Map<String, ClusterNode> getMonitoredNodes() {
        return monitoredNodes;
    }

    /**
     * 将节点标记为客观下线
     */
    public void markNodeAsObjectiveDown(String nodeId) {
        if (!objectiveDownNodes.contains(nodeId)) {
            objectiveDownNodes.add(nodeId);
            ClusterNode node = monitoredNodes.get(nodeId);
            boolean isMasterNode = node != null && node.isMaster();
            
            if (isMasterNode) {
                logger.info("主节点 " + nodeId + " 已确认客观下线");
            } else {
                logger.info("从节点 " + nodeId + " 已确认客观下线");
            }
        }
    }

    /**
     * 从客观下线列表中移除节点
     */
    public void removeNodeFromObjectiveDown(String nodeId) {
        objectiveDownNodes.remove(nodeId);
    }

    /**
     * 关闭所有节点连接
     */
    public void disconnectAllNodes() {
        for (Map.Entry<String, ClusterNode> entry : monitoredNodes.entrySet()) {
            try {
                entry.getValue().disconnect();
            } catch (Exception e) {
                logger.error("断开节点连接失败: " + entry.getKey(), e);
            }
        }
    }
} 