    package site.hnfy258.coder;

    import io.netty.channel.ChannelHandlerContext;
    import io.netty.channel.ChannelInboundHandlerAdapter;
    import org.apache.log4j.Logger;
    import site.hnfy258.RedisCore;
    import site.hnfy258.RedisCoreImpl;
    import site.hnfy258.command.Command;
    import site.hnfy258.command.CommandType;
    import site.hnfy258.protocal.*;
    import site.hnfy258.datatype.BytesWrapper;
    import site.hnfy258.aof.AOFHandler;
    import site.hnfy258.rdb.core.RDBHandler;

    import java.util.EnumSet;
    import java.util.Set;
    import java.util.concurrent.atomic.AtomicInteger;

    public class MyCommandHandler extends ChannelInboundHandlerAdapter {
        private static final Logger logger = Logger.getLogger(MyCommandHandler.class);
        private final RedisCoreImpl redisCore;
        private final AOFHandler aofHandler; // 可能为null
        private final RDBHandler rdbHandler;

        // 使用EnumSet提高查找效率
        private static final Set<CommandType> WRITE_COMMANDS = EnumSet.of(
                CommandType.SET, CommandType.DEL, CommandType.INCR, CommandType.MSET,
                CommandType.EXPIRE, CommandType.SADD, CommandType.SREM, CommandType.SPOP,
                CommandType.HSET, CommandType.HMEST, CommandType.HDEL,
                CommandType.LPUSH, CommandType.RPUSH, CommandType.LPOP, CommandType.RPOP, CommandType.LREM,
                CommandType.ZADD, CommandType.ZREM,CommandType.SELECT
        );

        // 用于统计处理中的命令数量，帮助诊断性能问题
        private static final AtomicInteger PROCESSING_COMMANDS = new AtomicInteger(0);
        // 用于控制日志频率，避免日志过多
        private static final int LOG_INTERVAL = 10000;
        private static final AtomicInteger LOG_COUNTER = new AtomicInteger(0);

        // 记录最后一次日志时间，用于限制日志频率
        private static volatile long lastLogTime = System.currentTimeMillis();

        public MyCommandHandler(RedisCore redisCore, AOFHandler aofHandler,RDBHandler rdbHandler) {
            this.redisCore = (RedisCoreImpl) redisCore;
            this.aofHandler = aofHandler; // 可能为null，表示AOF已禁用
            this.rdbHandler = rdbHandler;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            int currentProcessing = PROCESSING_COMMANDS.incrementAndGet();

            // 限制日志输出频率，避免日志过多影响性能
            if (LOG_COUNTER.incrementAndGet() % LOG_INTERVAL == 0) {
                long now = System.currentTimeMillis();
                if (now - lastLogTime > 5000) { // 至少5秒输出一次
                    ////logger.info("当前处理中的命令数: " + currentProcessing);
                    lastLogTime = now;
                }
            }

            try {
                if (msg instanceof SimpleString) {
                    ctx.writeAndFlush(msg);
                    return;
                }

                if (!(msg instanceof RespArray)) {
                    ctx.writeAndFlush(new Errors("ERR invalid message type"));
                    return;
                }

                RespArray command = (RespArray) msg;
                Resp[] array = command.getArray();

                if (array.length == 0 || !(array[0] instanceof BulkString)) {
                    ctx.writeAndFlush(new Errors("ERR invalid command format"));
                    return;
                }

                String commandName = ((BulkString) array[0]).getContent().toUtf8String().toUpperCase();

                try {
                    CommandType commandType = CommandType.valueOf(commandName);
                    Command cmd = commandType.getSupplier().apply(redisCore);
                    cmd.setContext(array);

                    Resp response = cmd.handle();

                    if (rdbHandler != null && WRITE_COMMANDS.contains(commandType)) {
                        rdbHandler.notifyDataChanged(redisCore.getCurrentDB().getId(), ((BulkString) array[1]).getContent());
                    }

                    // 如果AOF已启用且是写命令，写入AOF
                    if (aofHandler != null && WRITE_COMMANDS.contains(commandType)) {
                        try {
                            aofHandler.append(command);
                        } catch (Exception e) {
                            // 在高并发下，减少日志输出频率
                            if (LOG_COUNTER.get() % 100 == 0) {
                                logger.error("Failed to append command to AOF: " + e.getMessage());
                            }
                        }
                    }

                    // 使用无监听器的写入方式，减少回调开销
                    ctx.writeAndFlush(response);

                } catch (IllegalArgumentException e) {
                    // 未知命令
                    ctx.writeAndFlush(new Errors("ERR unknown command '" + commandName + "'"));
                } catch (Exception e) {
                    // 处理命令时的其他异常
                    logger.error("Error processing command: " + commandName, e);
                    ctx.writeAndFlush(new Errors("ERR internal error: " + e.getMessage()));
                }
            } finally {
                // 确保计数器减少
                PROCESSING_COMMANDS.decrementAndGet();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            // 减少日志输出频率，避免日志风暴
            if (LOG_COUNTER.get() % 100 == 0) {
                logger.error("Channel exception: " + cause.getMessage());
            }

            // 关闭连接
            ctx.close();
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            // 减少日志输出，只在开发环境或需要时启用
            if (logger.isDebugEnabled()) {
                logger.debug("Client connected: " + ctx.channel().remoteAddress());
            }

            BytesWrapper clientName = new BytesWrapper(("Client-" + ctx.channel().id().asShortText()).getBytes());
            redisCore.putClient(clientName, ctx.channel());

            // 限制日志频率
            if (LOG_COUNTER.incrementAndGet() % 100 == 0) {
                //logger.info("Connected clients: " + redisCore.getConnectedClientsCount());
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            // 减少日志输出，只在开发环境或需要时启用
            if (logger.isDebugEnabled()) {
                logger.debug("Client disconnected: " + ctx.channel().remoteAddress());
            }

            redisCore.disconnectClient(ctx.channel());

            // 限制日志频率
            if (LOG_COUNTER.incrementAndGet() % 100 == 0) {
                //logger.info("Connected clients: " + redisCore.getConnectedClientsCount());
            }
        }
    }
