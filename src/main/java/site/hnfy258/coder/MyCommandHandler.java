package site.hnfy258.coder;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.log4j.Logger;
import site.hnfy258.RedisCore;
import site.hnfy258.aof.AOFHandler;
import site.hnfy258.cluster.RedisCluster;
import site.hnfy258.cluster.replication.ReplicationHandler;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.command.CommandUtils;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.protocal.*;
import site.hnfy258.rdb.core.RDBHandler;
import site.hnfy258.server.MyRedisService;

import java.io.IOException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

@ChannelHandler.Sharable
public class MyCommandHandler extends SimpleChannelInboundHandler<Resp> {
    private static final Logger logger = Logger.getLogger(MyCommandHandler.class);
    private final RedisCore redisCore;
    private final AOFHandler aofHandler;
    private RDBHandler rdbHandler;
    private ReplicationHandler replicationHandler;

    // 定义不需要记录日志的命令
    private static final List<String> SILENT_COMMANDS = new ArrayList<>();
    
    static {
        SILENT_COMMANDS.add("SCAN");
        SILENT_COMMANDS.add("PING");
        SILENT_COMMANDS.add("INFO");
    }



    public MyCommandHandler(RedisCore core, AOFHandler aofHandler, RDBHandler rdbHandler, ReplicationHandler replicationHandler) {
        this.redisCore = core;
        this.aofHandler = aofHandler;
        this.rdbHandler = rdbHandler;
        this.replicationHandler = replicationHandler;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Resp msg) {
        if (msg instanceof RespArray) {
            RespArray array = (RespArray) msg;
            Resp response = processCommand(array);
            if (response != null) {
                ctx.writeAndFlush(response);
            }
        } else {
            ctx.writeAndFlush(new Errors("ERR unknown request type"));
        }
    }

    public Resp processCommand(RespArray commandArray) {
        if (commandArray.getArray().length == 0) {
            return new Errors("ERR empty command");
        }

        try {
            Resp[] array = commandArray.getArray();
            String commandName = ((BulkString) array[0]).getContent().toUtf8String().toUpperCase();
            CommandType commandType;

            try {
                commandType = CommandType.valueOf(commandName);
            } catch (IllegalArgumentException e) {
                return new Errors("ERR unknown command '" + commandName + "'");
            }

            // 检查是否是静默命令（不需要记录日志的命令）
            boolean isSilentCommand = CommandUtils.isSilentCommand(commandName);
            
            // 判断是否是写命令
            boolean isWriteCommand = CommandUtils.isWriteCommand(commandType);
            
            // 只有非静默命令才记录详细日志
            if (!isSilentCommand) {
                logger.info("处理命令: " + commandName + ", 是否是写命令: " + isWriteCommand + 
                        ", 是否有复制处理器: " + (replicationHandler != null));
            }

            // 判断是否需要在集群中处理（主要针对分片模式）
            if (shouldHandleInCluster(commandType, commandArray)) {
                return handleClusterCommand(commandType, commandArray);
            }

            // 本地执行命令
            Command command = commandType.getSupplier().apply(redisCore);
            command.setContext(array);
            Resp result = command.handle();

            // 触发RDB变更通知和AOF记录（仅对写命令）
            if (isWriteCommand && array.length > 1) {
                if (!isSilentCommand) {
                    logger.info("处理写命令: " + commandName + " 在数据库索引: " + redisCore.getCurrentDBIndex());
                }
                
                if (rdbHandler != null) {
                    rdbHandler.notifyDataChanged(redisCore.getCurrentDB().getId(), ((BulkString) array[1]).getContent());
                }

            }
            // 写命令处理：AOF记录
            if (aofHandler != null) {
                if(!commandType.equals(CommandType.BGREWRITEAOF)){
                    aofHandler.append(commandArray);
                    logger.info(aofHandler.getWriter(),aofHandler.getProcessor);
                }

            }
                
            // 主从复制：复制所有命令（不仅限于写命令）
            if (replicationHandler != null && array.length > 0) {
                // 记录要复制的命令
                if (logger.isDebugEnabled() && !isSilentCommand) {
                    String cmdStr = formatCommand(commandArray);
                    logger.debug("复制命令: " + cmdStr + 
                               ", 数据库索引: " + redisCore.getCurrentDBIndex());
                }
                
                // 将命令复制到所有从节点
                replicationHandler.handle(commandArray);
                
                // 只对非静默命令记录完成日志
                if (!isSilentCommand) {
                    logger.info("完成命令复制: " + commandName);
                }
            }

            return result;
        } catch (Exception e) {
            logger.error("Error processing command", e);
            return new Errors("ERR " + e.getMessage());
        }
    }

    private boolean shouldHandleInCluster(CommandType commandType, RespArray commandArray) {
        RedisCluster cluster = redisCore.getRedisService().getCluster();
        
        // 如果集群不存在或未启用分片，直接返回false
        if (cluster == null || !cluster.isShardingEnabled()) {
            return false;
        }
        
        // 对于SCAN命令，始终在本地执行，不使用集群路由
        if (commandType == CommandType.SCAN) {
            return false;
        }
        
        // 在启用了主从复制的情况下，所有写命令都在本地处理后通过复制机制同步到从节点
        if (CommandUtils.isWriteCommand(commandType) && replicationHandler != null) {
            return false;
        }
        
        // 在分片模式下，读命令按键路由到相应节点
        // 目前只处理GET命令，如果需要支持更多读命令，可以扩展这里
        return commandType == CommandType.GET;
    }

    private Resp handleClusterCommand(CommandType commandType, RespArray commandArray) {
        RedisCluster cluster = redisCore.getRedisService().getCluster();
        BytesWrapper key = ((BulkString) commandArray.getArray()[1]).getContent();
        String targetNodeId = cluster.getNodeForKey(key);
        String currentNodeId = redisCore.getRedisService().getCurrentNode().getId();

        if (currentNodeId.equals(targetNodeId)) {
            Command command = commandType.getSupplier().apply(redisCore);
            command.setContext(commandArray.getArray());
            Resp result = command.handle();

            // 确保本地执行时也触发RDB
            if (rdbHandler != null && CommandUtils.isWriteCommand(commandType) && commandArray.getArray().length > 1) {
                rdbHandler.notifyDataChanged(redisCore.getCurrentDB().getId(), key);
            }

            return result;
        } else {
            Resp result = forwardToTargetNode(targetNodeId, commandArray);
            return result;
        }
    }

    private Resp forwardToTargetNode(String targetNodeId, RespArray commandArray) {
        try {
            // 获取目标节点的服务实例
            RedisCluster cluster = redisCore.getRedisService().getCluster();
            MyRedisService targetService = cluster.getNode(targetNodeId);

            if (targetService != null) {
                // 转发命令到目标节点
                return targetService.executeCommand(commandArray);
            } else {
                return new Errors("ERR target node not available: " + targetNodeId);
            }
        } catch (Exception e) {
            logger.error("Error forwarding command to node " + targetNodeId, e);
            return new Errors("ERR forwarding failed: " + e.getMessage());
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        // 客户端断开连接时调用
        if (logger.isDebugEnabled()) {
            logger.debug("Client disconnected: " + ctx.channel().remoteAddress());
        }
        // 调用父类方法确保事件传播
        ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // 检查是否是客户端断开连接导致的异常
        if (isClientDisconnectException(cause)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Client connection closed: " + ctx.channel().remoteAddress(), cause);
            }
        } else {
            // 其他类型的异常仍然记录为错误
            logger.error("Error in command handler", cause);
            // 只有在通道仍然活跃时才尝试写入响应
            if (ctx.channel().isActive()) {
                ctx.writeAndFlush(new Errors("ERR " + cause.getMessage()));
            }
        }
        // 如果连接已经不可用，关闭它
        if (!ctx.channel().isActive()) {
            ctx.close();
        }
    }

    /**
     * 判断异常是否是由客户端断开连接引起的
     *
     * @param cause 异常
     * @return 如果是连接断开异常返回true
     */
    private boolean isClientDisconnectException(Throwable cause) {
        if (cause instanceof IOException) {
            String message = cause.getMessage();
            // 检查各种可能的连接断开消息
            return message != null && (
                message.contains("Connection reset by peer") ||
                message.contains("远程主机强迫关闭了一个现有的连接") ||
                message.contains("Broken pipe") ||
                message.contains("Connection refused") ||
                message.contains("Connection closed") ||
                cause instanceof SocketException
            );
        }
        return false;
    }

    // 将命令数组转换为可读字符串（用于日志）
    private String formatCommand(RespArray commandArray) {
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

    // 添加动态更新复制处理器的方法
    public void setReplicationHandler(ReplicationHandler replicationHandler) {
        this.replicationHandler = replicationHandler;
    }
}
