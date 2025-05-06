package site.hnfy258.sentinel;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.log4j.Logger;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.protocal.*;
import java.util.concurrent.CompletableFuture;

/**
 * 处理来自其他Sentinel的客户端连接
 */
public class SentinelClientHandler extends SimpleChannelInboundHandler<Resp> {
    private final Logger logger = Logger.getLogger(SentinelClientHandler.class);
    private final Sentinel sentinel;
    private final String neighborId;

    public SentinelClientHandler(Sentinel sentinel, String neighborId) {
        this.sentinel = sentinel;
        this.neighborId = neighborId;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Resp msg) {
        if (msg instanceof RespArray) {
            RespArray array = (RespArray) msg;
            processCommand(ctx, array);
            // 尝试处理可能的异步响应
            processAsyncResponse(array);
        } else if (msg instanceof SimpleString) {
            // 处理简单字符串响应，如PONG
            String content = ((SimpleString) msg).getContent();
            if ("PONG".equalsIgnoreCase(content)) {
                // 收到PONG响应后更新最后回复时间
                sentinel.getNeighborManager().updateNeighborLastReplyTime(neighborId);
                if (logger.isDebugEnabled()) {
                    logger.debug("收到Sentinel " + neighborId + " 的PONG响应");
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
                sentinel.getNeighborManager().updateNeighborLastReplyTime(neighborId);
            }
            // 处理IS-MASTER-DOWN-BY-ADDR命令
            else if ("SENTINEL".equals(cmdName) && array.length >= 3 && 
                    "IS-MASTER-DOWN-BY-ADDR".equals(((BulkString) array[1]).getContent().toUtf8String())) {
                String masterId = ((BulkString) array[2]).getContent().toUtf8String();
                boolean isDown = sentinel.getNodeManager().isMasterSubjectivelyDown(masterId);
                
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
        } catch (Exception e) {
            logger.error("处理来自邻居Sentinel的命令失败", e);
        }
    }

    /**
     * 处理异步响应，完成相应的CompletableFuture
     */
    private void processAsyncResponse(RespArray response) {
        try {
            Resp[] array = response.getArray();
            if (array.length < 2) {
                return;  // 至少需要结果和请求ID
            }
            
            // 尝试解析响应
            if (array[0] instanceof BulkString && array[1] instanceof BulkString) {
                String resultValue = ((BulkString) array[0]).getContent().toUtf8String();
                String requestId = ((BulkString) array[1]).getContent().toUtf8String();
                
                // 查找并完成对应的Future
                CompletableFuture<Boolean> future = sentinel.getNeighborManager().getPendingRequest(requestId);
                if (future != null && !future.isDone()) {
                    boolean isDown = "1".equals(resultValue);
                    future.complete(isDown);
                    if (logger.isDebugEnabled()) {
                        logger.debug("收到异步IS-MASTER-DOWN-BY-ADDR响应, requestId=" + requestId + ", isDown=" + isDown);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("处理异步响应失败", e);
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        logger.info("与邻居Sentinel建立连接: " + ctx.channel().remoteAddress());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        logger.info("与邻居Sentinel连接断开: " + ctx.channel().remoteAddress());
        sentinel.getNeighborManager().removeNeighborChannel(neighborId);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("与邻居Sentinel通信异常", cause);
        ctx.close();
    }
}