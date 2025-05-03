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
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.protocal.*;
import site.hnfy258.rdb.core.RDBHandler;
import site.hnfy258.server.MyRedisService;

import java.io.IOException;
import java.net.SocketException;
import java.util.EnumSet;
import java.util.Set;

@ChannelHandler.Sharable
public class MyCommandHandler extends SimpleChannelInboundHandler<Resp> {
    private static final Logger logger = Logger.getLogger(MyCommandHandler.class);
    private final RedisCore redisCore;
    private final AOFHandler aofHandler;
    private final RDBHandler rdbHandler;

    private final ReplicationHandler replicationHandler;



    // 使用EnumSet提高查找效率
    private static final Set<CommandType> WRITE_COMMANDS = EnumSet.of(
            CommandType.SET, CommandType.DEL, CommandType.INCR, CommandType.MSET,
            CommandType.EXPIRE, CommandType.SADD, CommandType.SREM, CommandType.SPOP,
            CommandType.HSET, CommandType.HMEST, CommandType.HDEL,
            CommandType.LPUSH, CommandType.RPUSH, CommandType.LPOP, CommandType.RPOP, CommandType.LREM,
            CommandType.ZADD, CommandType.ZREM,CommandType.SELECT
    );

    public MyCommandHandler(RedisCore redisCore, AOFHandler aofHandler, RDBHandler rdbHandler, ReplicationHandler replicationHandler) {
        this.redisCore = redisCore;
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

            if (shouldHandleInCluster(commandType, commandArray)) {
                return handleClusterCommand(commandType, commandArray);
            }

            Command command = commandType.getSupplier().apply(redisCore);
            command.setContext(array);

            Resp result = command.handle();

            if (rdbHandler != null && WRITE_COMMANDS.contains(commandType) && array.length > 1) {
                rdbHandler.notifyDataChanged(redisCore.getCurrentDB().getId(), ((BulkString) array[1]).getContent());
            }

            // 如果启用了AOF，记录命令
           if (aofHandler != null && WRITE_COMMANDS.contains(commandType)){
                aofHandler.append(commandArray);
            }

           if(replicationHandler != null && WRITE_COMMANDS.contains(commandType)){
               replicationHandler.handle(commandArray);
               System.out.println("replicationHandler send command"+ commandArray);
           }

            return result;
        } catch (Exception e) {
            logger.error("Error processing command", e);
            return new Errors("ERR " + e.getMessage());
        }
    }

    private boolean shouldHandleInCluster(CommandType commandType, RespArray commandArray) {
        RedisCluster cluster = redisCore.getRedisService().getCluster();
        return cluster != null && cluster.isShardingEnabled() &&
                (commandType == CommandType.GET || commandType == CommandType.SET);
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
            if (rdbHandler != null && WRITE_COMMANDS.contains(commandType) && commandArray.getArray().length > 1) {
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
}
