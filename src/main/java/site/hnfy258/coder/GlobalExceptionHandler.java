package site.hnfy258.coder;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.DecoderException;
import org.apache.log4j.Logger;
import site.hnfy258.protocal.Errors;

import java.io.IOError;

public class GlobalExceptionHandler extends ChannelInboundHandlerAdapter {
    private static final Logger logger = Logger.getLogger(GlobalExceptionHandler.class);
    
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("未处理的异常", cause);
        
        // 根据异常类型发送适当的错误响应
        if (cause instanceof DecoderException) {
            ctx.writeAndFlush(new Errors("ERR 协议解析错误"));
        } else if (cause instanceof OutOfMemoryError) {
            ctx.writeAndFlush(new Errors("ERR 服务器内存不足"));
        } else {
            ctx.writeAndFlush(new Errors("ERR 内部服务器错误"));
        }
        
        // 对于严重错误，可以选择关闭连接
        if (cause instanceof OutOfMemoryError || cause instanceof IOError) {
            ctx.close();
        }
    }
}