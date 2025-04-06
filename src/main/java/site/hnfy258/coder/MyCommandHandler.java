package site.hnfy258.coder;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import site.hnfy258.RedisCore;
import site.hnfy258.RedisCoreImpl;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.protocal.*;
import site.hnfy258.datatype.BytesWrapper;

public class MyCommandHandler extends ChannelInboundHandlerAdapter {
    private final RedisCoreImpl redisCore;

    public MyCommandHandler(RedisCore redisCore) {
        this.redisCore = (RedisCoreImpl) redisCore;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof RespArray) {
            RespArray command = (RespArray) msg;
            Resp[] array = command.getArray();

            if (array.length > 0 && array[0] instanceof BulkString) {
                String commandName = ((BulkString) array[0]).getContent().toUtf8String().toUpperCase();

                try {
                    CommandType commandType = CommandType.valueOf(commandName);
                    Command cmd = commandType.getSupplier().apply(redisCore);
                    cmd.setContext(array);

                    Resp response = cmd.handle();
                    ctx.writeAndFlush(response).addListener(future -> {
                        if (future.isSuccess()) {
                            System.out.println("响应发送成功");
                        } else {
                            System.err.println("响应发送失败: " + future.cause());
                        }
                    });
                } catch (IllegalArgumentException e) {
                    System.err.println("未知命令: " + commandName);
                    ctx.writeAndFlush(new Errors("ERR unknown command '" + commandName + "'"));
                }
            } else {
                System.err.println("无效的命令格式");
                ctx.writeAndFlush(new Errors("ERR invalid command format"));
            }
        } else {
            System.err.println("无效的消息类型");
            ctx.writeAndFlush(new Errors("ERR invalid message type"));
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        System.err.println("处理异常: " + cause.getMessage());
        cause.printStackTrace();
        ctx.close();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        System.out.println("客户端连接: " + ctx.channel().remoteAddress());
        BytesWrapper clientName = new BytesWrapper(("Client-" + ctx.channel().id().asShortText()).getBytes());
        redisCore.putClient(clientName, ctx.channel());
        System.out.println("当前连接的客户端数: " + redisCore.getConnectedClientsCount());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        System.out.println("客户端断开: " + ctx.channel().remoteAddress());
        redisCore.disconnectClient(ctx.channel());
        System.out.println("当前连接的客户端数: " + redisCore.getConnectedClientsCount());
    }
}
