package site.hnfy258.sentinel;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.log4j.Logger;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.protocal.*;

/**
 * 处理接收到的连接请求
 */
public class SentinelServerHandler extends SimpleChannelInboundHandler<Resp> {
    private final Logger logger = Logger.getLogger(SentinelServerHandler.class);
    private final Sentinel sentinel;
    private String clientId = null;

    public SentinelServerHandler(Sentinel sentinel) {
        this.sentinel = sentinel;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Resp msg) {
        if (msg instanceof RespArray) {
            RespArray array = (RespArray) msg;
            processCommand(ctx, array);
        } else if (msg instanceof SimpleString) {
            // 处理简单字符串响应，如PONG
            String content = ((SimpleString) msg).getContent();
            if ("PONG".equalsIgnoreCase(content) && clientId != null) {
                // 收到PONG响应后更新最后回复时间
                sentinel.updateNodeLastReplyTime(clientId);
                if (logger.isDebugEnabled()) {
                    logger.debug("收到客户端 " + clientId + " 的PONG响应");
                }
            }
        }
    }

    private void processCommand(ChannelHandlerContext ctx, RespArray command) {
        try {
            Resp[] array = command.getArray();
            if (array.length == 0) {
                return;
            }

            // 解析命令
            String cmdName = ((BulkString) array[0]).getContent().toUtf8String().toUpperCase();

            // 处理PING命令
            if ("PING".equals(cmdName)) {
                ctx.writeAndFlush(new SimpleString("PONG"));
                if (clientId != null) {
                    sentinel.updateNodeLastReplyTime(clientId);
                }
            }
            // 处理SENTINEL命令
            else if ("SENTINEL".equals(cmdName)) {
                handleSentinelCommand(ctx, array);
            }
            // 处理其他命令...
        } catch (Exception e) {
            logger.error("处理命令失败", e);
            ctx.writeAndFlush(new Errors("ERR " + e.getMessage()));
        }
    }

    private void handleSentinelCommand(ChannelHandlerContext ctx, Resp[] array) {
        if (array.length < 2) {
            ctx.writeAndFlush(new Errors("ERR wrong number of arguments for 'sentinel' command"));
            return;
        }

        String subCommand = ((BulkString) array[1]).getContent().toUtf8String().toUpperCase();

        // 处理SENTINEL HELLO命令（注册节点）
        if ("HELLO".equals(subCommand) && array.length >= 3) {
            clientId = ((BulkString) array[2]).getContent().toUtf8String();
            ctx.writeAndFlush(new SimpleString("OK"));
            logger.info("客户端注册ID: " + clientId);
        }
        // 处理IS-MASTER-DOWN-BY-ADDR命令
        else if ("IS-MASTER-DOWN-BY-ADDR".equals(subCommand) && array.length >= 3) {
            String masterId = ((BulkString) array[2]).getContent().toUtf8String();
            boolean isDown = sentinel.getSubjectiveDownNodes().contains(masterId);
            
            // 检查是否有请求ID
            String requestId = null;
            if (array.length >= 4) {
                requestId = ((BulkString) array[3]).getContent().toUtf8String();
            }
            
            // 返回节点状态
            RespArray response;
            if (requestId != null) {
                response = new RespArray(new Resp[]{
                        new BulkString(new BytesWrapper((isDown ? "1" : "0").getBytes(BytesWrapper.CHARSET))),
                        new BulkString(new BytesWrapper(requestId.getBytes(BytesWrapper.CHARSET)))
                });
            } else {
                response = new RespArray(new Resp[]{
                        new BulkString(new BytesWrapper((isDown ? "1" : "0").getBytes(BytesWrapper.CHARSET)))
                });
            }
            ctx.writeAndFlush(response);
        }
        // 处理其他SENTINEL子命令...
        else {
            ctx.writeAndFlush(new Errors("ERR unknown sentinel subcommand '" + subCommand + "'"));
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        logger.info("新连接建立: " + ctx.channel().remoteAddress());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        logger.info("连接关闭: " + ctx.channel().remoteAddress());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("连接异常", cause);
        ctx.close();
    }
} 