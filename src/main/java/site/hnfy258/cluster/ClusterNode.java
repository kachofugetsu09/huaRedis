package site.hnfy258.cluster;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.log4j.Logger;
import site.hnfy258.RedisCore;
import site.hnfy258.coder.MyCommandHandler;
import site.hnfy258.coder.MyDecoder;
import site.hnfy258.coder.MyResponseEncoder;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.command.CommandUtils;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.protocal.Errors;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;
import site.hnfy258.protocal.SimpleString;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.server.MyRedisService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * 重构后的ClusterNode类，整合了ClusterClient的功能
 * 实现了Redis原生架构风格的节点设计
 */
public class ClusterNode {
    private static final Logger logger = Logger.getLogger(ClusterNode.class);
    
    // 节点基本信息
    private final String id;
    private final String ip;
    private final int port;
    private boolean isMaster;
    private String masterId;              // 如果是从节点，这里存储其主节点的ID
    private List<ClusterNode> slaves;     // 该主节点的从节点列表
    
    // Redis服务相关
    private MyRedisService service;
    private RedisCore redisCore;
    
    // 网络连接相关（原ClusterClient功能）
    private Channel channel;
    private EventLoopGroup group;
    private boolean connected = false;
    
    /**
     * 创建一个新的集群节点
     * @param nodeId 节点唯一标识
     * @param ip 节点IP地址
     * @param port 节点端口
     * @param isMaster 是否为主节点
     */
    public ClusterNode(String nodeId, String ip, int port, boolean isMaster) {
        this.id = nodeId;
        this.ip = ip;
        this.port = port;
        this.isMaster = isMaster;
        this.slaves = new ArrayList<>();
    }
    
    /**
     * 设置当前节点的Redis核心
     * @param redisCore Redis核心实例
     */
    public void setRedisCore(RedisCore redisCore) {
        this.redisCore = redisCore;
    }
    
    /**
     * 获取当前节点的Redis核心
     * @return Redis核心实例
     */
    public RedisCore getRedisCore() {
        return this.redisCore;
    }

    /**
     * 连接到目标节点
     * @return 连接完成的Future
     */
    public CompletableFuture<Void> connect() {
        if (connected && channel != null && channel.isActive()) {
            return CompletableFuture.completedFuture(null);
        }
        
        CompletableFuture<Void> future = new CompletableFuture<>();
        
        group = new NioEventLoopGroup();
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new MyDecoder());
                        pipeline.addLast(new MyResponseEncoder());
                        pipeline.addLast(new ClusterNodeHandler(redisCore));
                    }
                });

        connectWithRetry(bootstrap, ip, port, future, 3, 1000);
        return future.thenApply(v -> {
            connected = true;
            return null;
        });
    }

    private void connectWithRetry(Bootstrap bootstrap, String host, int port,
                                 CompletableFuture<Void> future, int retries, long delayMs) {
        bootstrap.connect(host, port).addListener((ChannelFutureListener) f -> {
            if (f.isSuccess()) {
                channel = f.channel();
                future.complete(null);
            } else if (retries > 0) {
                logger.info(String.format("连接到 %s:%d 失败，剩余重试次数: %d. 正在重试...",
                        host, port, retries));
                f.channel().eventLoop().schedule(() ->
                                connectWithRetry(bootstrap, host, port, future, retries - 1, delayMs),
                        delayMs, TimeUnit.MILLISECONDS);
            } else {
                future.completeExceptionally(f.cause());
            }
        });
    }

    /**
     * 发送消息到节点
     * @param resp RESP消息
     */
    public void sendMessage(Resp resp) {
        if (channel != null && channel.isActive()) {
            channel.writeAndFlush(resp);
        } else {
            logger.error("通道未激活，无法发送消息: " + formatMessage(resp));
        }
    }

    /**
     * 异步发送消息到节点
     * @param resp RESP消息
     * @return 发送完成的Future
     */
    public CompletableFuture<Void> sendMessageAsync(Resp resp) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        if (channel != null && channel.isActive()) {
            channel.writeAndFlush(resp).addListener(f -> {
                if (f.isSuccess()) {
                    future.complete(null);
                } else {
                    future.completeExceptionally(f.cause());
                }
            });
        } else {
            future.completeExceptionally(new IllegalStateException("通道未激活，无法发送消息"));
        }
        return future;
    }

    /**
     * 断开与目标节点的连接
     */
    public void disconnect() {
        if (channel != null) {
            channel.close();
        }
        if (group != null) {
            group.shutdownGracefully();
        }
        connected = false;
    }

    /**
     * 检查连接是否活跃
     * @return 如果连接活跃返回true
     */
    public boolean isActive() {
        return channel != null && channel.isActive();
    }

    /**
     * 添加从节点到主节点（仅主节点有效）
     * @param slave 从节点
     */
    public void addSlave(ClusterNode slave) {
        if (slaves == null) {
            slaves = new ArrayList<>();
        }

        // 检查是否已经包含该从节点
        if (slave != null && !containsSlave(slave)) {
            slaves.add(slave);

            slave.setMasterId(this.id);
            slave.setMaster(false);
        }
    }

    /**
     * 获取节点ID
     * @return 节点ID
     */
    public String getId() {
        return id;
    }

    /**
     * 获取节点端口
     * @return 端口号
     */
    public int getPort() {
        return port;
    }

    /**
     * 获取节点IP
     * @return IP地址
     */
    public String getIp() {
        return ip;
    }

    /**
     * 检查是否为主节点
     * @return 如果是主节点返回true
     */
    public boolean isMaster() {
        return isMaster;
    }

    /**
     * 获取从节点列表
     * @return 从节点列表
     */
    public List<ClusterNode> getSlaves() {
        return slaves;
    }

    /**
     * 获取关联的Redis服务
     * @return Redis服务实例
     */
    public MyRedisService getService() {
        return service;
    }

    /**
     * 设置关联的Redis服务
     * @param service Redis服务实例
     */
    public void setService(MyRedisService service) {
        this.service = service;
        if (service != null) {
            this.redisCore = service.getRedisCore();
        }
    }

    /**
     * 设置节点的主从状态
     * @param b 如果为true则设为主节点，否则设为从节点
     */
    public void setMaster(boolean b) {
        isMaster = b;

        // 初始化从节点列表（如果还没有）
        if (isMaster && slaves == null) {
            slaves = new ArrayList<>();
        }

        // 如果设置为从节点，清空从节点列表
        if (!isMaster) {
            if (slaves != null) {
                slaves.clear();
            }
        }
    }

    /**
     * 获取主节点ID（仅从节点有效）
     * @return 主节点ID
     */
    public String getMasterId() {
        return masterId;
    }

    /**
     * 设置主节点ID（将此节点设为从节点）
     * @param masterId 主节点ID
     */
    public void setMasterId(String masterId) {
        this.masterId = masterId;
        if (masterId != null) {
            this.isMaster = false;
        }
    }

    /**
     * 从从节点列表中移除指定的从节点
     *
     * @param slave 要移除的从节点
     * @return 如果成功移除返回true，否则返回false
     */
    public boolean removeSlave(ClusterNode slave) {
        if (slaves == null || slave == null) {
            return false;
        }

        // 使用节点ID进行比较，因为可能传入的是不同的对象实例
        for (int i = 0; i < slaves.size(); i++) {
            ClusterNode currentSlave = slaves.get(i);
            if (currentSlave != null && currentSlave.getId().equals(slave.getId())) {
                slaves.remove(i);
                return true;
            }
        }

        return false;
    }

    /**
     * 检查从节点列表中是否已包含指定从节点
     * @param slave 要检查的从节点
     * @return 如果已包含则返回true
     */
    private boolean containsSlave(ClusterNode slave) {
        if (slave == null || slaves == null) {
            return false;
        }

        for (ClusterNode existingSlave : slaves) {
            if (existingSlave != null && existingSlave.getId().equals(slave.getId())) {
                return true;
            }
        }

        return false;
    }
    
    /**
     * 将RESP消息格式化为字符串，便于日志输出
     */
    private String formatMessage(Resp message) {
        if (message == null) {
            return "null";
        }
        if (message instanceof RespArray) {
            RespArray array = (RespArray) message;
            StringBuilder sb = new StringBuilder("[");
            Resp[] respArray = array.getArray();
            for (int i = 0; i < respArray.length && i < 3; i++) {
                if (i > 0) sb.append(", ");
                sb.append(respArray[i].toString());
            }
            if (respArray.length > 3) {
                sb.append("...(").append(respArray.length).append(" items)");
            }
            sb.append("]");
            return sb.toString();
        } else {
            return message.toString();
        }
    }

    /**
     * 处理从节点接收到的命令
     */
    private static class ClusterNodeHandler extends SimpleChannelInboundHandler<Resp> {
        private static final Logger logger = Logger.getLogger(ClusterNodeHandler.class);
        private final RedisCore redisCore;
        private int currentDbIndex = 0;

        public ClusterNodeHandler(RedisCore redisCore) {
            this.redisCore = redisCore;
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
        
        private Resp processCommand(RespArray commandArray) {
            Resp[] array = commandArray.getArray();
            if (array.length == 0) {
                return new Errors("ERR empty command");
            }
            
            try {
                String commandName = ((BulkString) array[0]).getContent().toUtf8String().toUpperCase();
                
                // 对于频繁执行的命令，减少日志输出
                boolean isFrequentCommand = "PING".equals(commandName) || 
                                           "INFO".equals(commandName) || 
                                           "SCAN".equals(commandName);
                
                // 始终记录写命令的处理
                boolean isImportantCommand = CommandUtils.isWriteCommand(commandName);
                
                // 记录详细日志，特别是写命令
                if (isImportantCommand) {
                    logger.info("从节点收到命令: " + formatCommand(commandArray) + 
                                ", 当前DB: " + currentDbIndex + 
                                ", RedisCore DB: " + redisCore.getCurrentDBIndex());
                }
                
                CommandType commandType;

                try {
                    commandType = CommandType.valueOf(commandName);
                } catch (IllegalArgumentException e) {
                    return new Errors("ERR unknown command '" + commandName + "'");
                }

                // 特殊处理SELECT命令
                if (commandType == CommandType.SELECT) {
                    if (array.length > 1 && array[1] instanceof BulkString) {
                        try {
                            String indexStr = ((BulkString) array[1]).getContent().toUtf8String();
                            int newDbIndex = Integer.parseInt(indexStr);

                            currentDbIndex = newDbIndex;

                            redisCore.selectDB(currentDbIndex);
                            logger.info("从节点切换到数据库: " + currentDbIndex);
                            
                            return new SimpleString("OK");
                        } catch (NumberFormatException e) {
                            logger.error("无效的数据库索引", e);
                            return new Errors("ERR invalid DB index");
                        } catch (IllegalArgumentException e) {
                            logger.error("数据库索引超出范围", e);
                            return new Errors("ERR invalid DB index");
                        }
                    } else {
                        return new Errors("ERR wrong number of arguments for 'select' command");
                    }
                }

                try {
                    // 获取Redis核心当前数据库索引
                    int currentRedisDbIndex = redisCore.getCurrentDBIndex();
                    
                    // 确保数据保存所在的数据库索引一致
                    if (currentRedisDbIndex != currentDbIndex) {
                        logger.info("从节点数据库索引不一致，强制同步: " + currentRedisDbIndex + " -> " + currentDbIndex);
                        redisCore.selectDB(currentDbIndex);
                    }
                } catch (Exception e) {
                    logger.error("切换数据库失败，恢复到默认数据库0: " + e.getMessage(), e);
                    currentDbIndex = 0;
                    redisCore.selectDB(0);
                }
                
                // 执行命令
                if (!isFrequentCommand || isImportantCommand) {
                    logger.info("从节点开始执行命令: " + commandName + 
                                ", 参数长度: " + array.length + 
                                ", 当前DB: " + currentDbIndex);
                }
                
                // 对于写命令，打印额外调试信息
                if (isImportantCommand) {
                    try {
                        if (array.length > 1) {
                            String keyStr = ((BulkString) array[1]).getContent().toUtf8String();
                            Object currentValue = redisCore.get(((BulkString) array[1]).getContent());
                            logger.info("写命令前数据状态: key=" + keyStr + 
                                       ", 在DB:" + currentDbIndex + 
                                       ", 当前值类型: " + (currentValue != null ? currentValue.getClass().getSimpleName() : "null"));
                        }
                    } catch (Exception e) {
                        logger.warn("读取键值数据失败: " + e.getMessage());
                    }
                }

                // 执行命令
                Command command = commandType.getSupplier().apply(redisCore);
                command.setContext(array);
                Resp result = command.handle();

                // 写命令执行后记录日志
                if (isImportantCommand) {
                    logger.info("从节点完成命令: " + commandName + 
                               ", 数据库: " + currentDbIndex + 
                               ", 结果: " + formatResult(result));
                }

                return result;
            } catch (Exception e) {
                logger.error("从节点执行命令发生错误", e);
                return new Errors("ERR " + e.getMessage());
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            logger.error("处理命令时发生异常", cause);
            ctx.close();
        }
        
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
        
        private String formatResult(Resp result) {
            if (result == null) {
                return "null";
            }
            
            if (result instanceof SimpleString) {
                return ((SimpleString) result).getContent();
            } else if (result instanceof BulkString) {
                BytesWrapper content = ((BulkString) result).getContent();
                if (content == null) {
                    return "(nil)";
                }
                if (content.getBytes().length > 50) {
                    return content.toUtf8String().substring(0, 47) + "...";
                }
                return content.toUtf8String();
            } else if (result instanceof RespArray) {
                RespArray array = (RespArray) result;
                if (array.getArray().length == 0) {
                    return "[]";
                }
                return "[数组:" + array.getArray().length + "个元素]";
            } else if (result instanceof Errors) {
                return "错误:" + ((Errors) result).getContent();
            } else {
                return result.toString();
            }
        }
    }
}