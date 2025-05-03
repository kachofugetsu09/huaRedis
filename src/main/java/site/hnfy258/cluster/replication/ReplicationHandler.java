package site.hnfy258.cluster.replication;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.log4j.Logger;
import site.hnfy258.RedisCore;
import site.hnfy258.cluster.ClusterClient;
import site.hnfy258.cluster.ClusterNode;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;
import site.hnfy258.protocal.SimpleString;
import site.hnfy258.server.MyRedisService;
import site.hnfy258.command.CommandUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

public class ReplicationHandler {
    private static final Logger logger = Logger.getLogger(ReplicationHandler.class);
    
    // 定义不需要复制和不需要记录日志的命令
    private static final List<String> NON_REPLICATE_COMMANDS = new ArrayList<>();
    
    static {
        NON_REPLICATE_COMMANDS.add("SCAN");
        NON_REPLICATE_COMMANDS.add("PING");
        NON_REPLICATE_COMMANDS.add("INFO");
    }
    
    private ClusterNode masterNode;
    private MyRedisService masterService;
    // 记录每个从节点的当前数据库索引
    private Map<String, Integer> slaveDbIndices = new HashMap<>();

    public ReplicationHandler(ClusterNode masterNode){
        if (masterNode == null) {
            throw new IllegalArgumentException("Master node cannot be null");
        }
        this.masterNode = masterNode;
        this.masterService = masterNode.getService();
        
        logger.info("ReplicationHandler初始化: 主节点=" + masterNode.getId() + 
                   ", 从节点数=" + (masterNode.getSlaves() != null ? masterNode.getSlaves().size() : 0));
    }

    // 获取命令的可读表示
    public String formatCommand(RespArray commandArray) {
        if (commandArray == null || commandArray.getArray().length == 0) {
            return "[]";
        }
        
        Resp[] array = commandArray.getArray();
        StringBuilder sb = new StringBuilder();
        
        // 尝试获取命令名称
        if (array[0] instanceof BulkString) {
            String cmdName = ((BulkString) array[0]).getContent().toUtf8String();
            sb.append(cmdName.toUpperCase());
            
            // 添加参数
            for (int i = 1; i < array.length && i < 4; i++) { // 最多显示前3个参数
                if (array[i] instanceof BulkString) {
                    sb.append(" ").append(((BulkString) array[i]).getContent().toUtf8String());
                } else {
                    sb.append(" [非字符串参数]");
                }
            }
            
            // 如果参数太多，显示省略号
            if (array.length > 4) {
                sb.append(" ...");
            }
            
            // 显示总参数数量
            sb.append(" (共").append(array.length - 1).append("个参数)");
        } else {
            sb.append(commandArray.toString());
        }
        
        return sb.toString();
    }

    private boolean isSelectCommand(RespArray commandArray) {
        Resp[] array = commandArray.getArray();
        if (array.length > 0 && array[0] instanceof BulkString) {
            String cmd = ((BulkString) array[0]).getContent().toUtf8String().toUpperCase();
            return "SELECT".equals(cmd);
        }
        return false;
    }

    private int getSelectedDbIndex(RespArray commandArray) {
        Resp[] array = commandArray.getArray();
        if (array.length > 1 && array[1] instanceof BulkString) {
            String indexStr = ((BulkString) array[1]).getContent().toUtf8String();
            try {
                return Integer.parseInt(indexStr);
            } catch (NumberFormatException e) {
                logger.error("Invalid DB index format: " + indexStr, e);
            }
        }
        return 0; // 默认数据库索引
    }

    private RespArray createSelectCommand(int dbIndex) {
        Resp[] array = new Resp[2];
        array[0] = new BulkString(new BytesWrapper("SELECT".getBytes(BytesWrapper.CHARSET)));
        array[1] = new BulkString(new BytesWrapper(String.valueOf(dbIndex).getBytes(BytesWrapper.CHARSET)));
        return new RespArray(array);
    }

    public void handle(RespArray commandArray) {
        try {
            // 检查主服务是否可用
            if (masterService == null) {
                logger.error("主服务为空，无法复制命令");
                return;
            }

            // 检查是否有从节点
            if (masterNode.getSlaves() == null || masterNode.getSlaves().isEmpty()) {
                // 无需冗余日志
                return;
            }
            
            // 检查是否是不需要复制的命令类型
            Resp[] array = commandArray.getArray();
            if (array.length == 0) {
                return;
            }
            
            if (array[0] instanceof BulkString) {
                String cmd = ((BulkString) array[0]).getContent().toUtf8String().toUpperCase();
                
                // 某些特定命令不需要复制
                if (CommandUtils.isNonReplicateCommand(cmd)) {
                    return;
                }
                
                // 获取主节点上的当前数据库索引
                RedisCore redisCore = masterService.getRedisCore();
                int currentDbIndex = redisCore.getCurrentDB().getId();
                
                // 检查命令是否是SELECT命令
                boolean isSelectCommand = isSelectCommand(commandArray);
                int selectDbIndex = -1;
                
                // 如果是SELECT命令，获取要切换的数据库索引
                if (isSelectCommand) {
                    selectDbIndex = getSelectedDbIndex(commandArray);
                }

                // 获取命令名字用于日志记录
                String commandName = ((BulkString) array[0]).getContent().toUtf8String().toUpperCase();
                
                // 检查是否是写命令
                boolean isWriteCommand = CommandUtils.isWriteCommand(commandName);
                
                // 广播命令到所有从节点
                int successCount = 0;
                List<String> failedSlaves = new ArrayList<>();
                
                for (ClusterNode slaveNode : masterNode.getSlaves()) {
                    if (slaveNode == null) continue;
                    String slaveId = slaveNode.getId();
                    
                    try {
                        // 获取从节点的客户端连接
                        ClusterClient slaveClient = masterService.getClusterClient(slaveId);
                        
                        // 如果没有连接可用，跳过该从节点
                        if (slaveClient == null || !slaveClient.isActive()) {
                            // 简化日志，记录关键信息
                            logger.warn("与从节点 " + slaveId + " 的连接不可用，无法复制命令 " + 
                                      (slaveClient == null ? "(连接为空)" : "(连接不活跃)"));
                            
                            Map<String, ClusterClient> clients = masterService.getClusterClients();
                            if (clients != null && !clients.isEmpty()) {
                                logger.warn("主节点可用连接: " + String.join(", ", clients.keySet()));
                            }
                            
                            failedSlaves.add(slaveId);
                            continue;
                        }
                        
                        // 获取从节点当前数据库索引，如果没有记录则默认为0
                        Integer slaveDbIndex = slaveDbIndices.getOrDefault(slaveId, 0);
                        
                        // 如果是SELECT命令，直接发送并更新索引
                        if (isSelectCommand) {
                            slaveClient.sendMessage(commandArray);
                            // 更新记录的从节点数据库索引
                            slaveDbIndices.put(slaveId, selectDbIndex);
                            successCount++;
                        } else {
                            // 如果数据库索引不匹配，先发送SELECT命令
                            if (slaveDbIndex != currentDbIndex) {
                                RespArray selectCommand = createSelectCommand(currentDbIndex);
                                slaveClient.sendMessage(selectCommand);
                                slaveDbIndices.put(slaveId, currentDbIndex);
                            }
                            
                            // 发送实际命令
                            slaveClient.sendMessage(commandArray);
                            successCount++;
                        }
                    } catch (Exception e) {
                        logger.error("复制命令到从节点 " + slaveId + " 失败: " + e.getMessage());
                        failedSlaves.add(slaveId);
                    }
                }
                
                // 重要命令记录复制结果
                if (isWriteCommand || isSelectCommand) {
                    logger.info("命令 " + commandName + " 复制结果: 成功=" + successCount + 
                               ", 失败=" + failedSlaves.size() +
                               (failedSlaves.isEmpty() ? "" : ", 失败节点: " + String.join(",", failedSlaves)));
                }
            } else {
                logger.warn("命令数组的第一个元素不是BulkString，无法确定命令类型");
            }
        } catch (Exception e) {
            logger.error("复制处理器出错: " + e.getMessage(), e);
        }
    }
}
