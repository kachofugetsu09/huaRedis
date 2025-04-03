package site.hnfy258.coder;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import site.hnfy258.command.Command;
import site.hnfy258.protocal.*;

public class MyCommandHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        System.out.println("接收到消息类型: " + msg.getClass().getName());

        if (msg instanceof RespArray) {
            RespArray command = (RespArray) msg;
            Resp[] array = command.getArray();

            if (array.length > 0 && array[0] instanceof BulkString) {
                String commandName = ((BulkString) array[0]).getContent().toUtf8String().toUpperCase();
                System.out.println("接收到命令: " + commandName);

                switch (commandName) {
                    case "PING":
                        System.out.println("发送PONG响应");
                        ctx.writeAndFlush(new SimpleString("PONG"));
                        break;
                    // 其他待定
                    default:
                        System.out.println("未知命令: " + commandName);
                        ctx.writeAndFlush(new Errors("ERR unknown command '" + commandName + "'"));
                }
            } else {
                System.out.println("无效命令格式");
            }
        } else {
            System.out.println("不是RespArray类型");
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
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        System.out.println("客户端断开: " + ctx.channel().remoteAddress());
    }
}
