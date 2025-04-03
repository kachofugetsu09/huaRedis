package site.hnfy258.coder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import site.hnfy258.protocal.Resp;

import java.util.List;

public class MyDecoder extends ByteToMessageDecoder {
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        try {
            if (in.readableBytes() > 0) {
                System.out.println("读取到字节: " + in.readableBytes());
                // 标记当前读取位置
                in.markReaderIndex();
                try {
                    Resp decoded = Resp.decode(in);
                    if (decoded != null) {
                        System.out.println("解码完成: " + decoded.getClass().getSimpleName());
                        out.add(decoded);
                    } else {
                        // 如果未能解码完整消息，则重置到标记位置
                        in.resetReaderIndex();
                    }
                } catch (Exception e) {
                    System.err.println("解码异常: " + e.getMessage());
                    e.printStackTrace();
                    in.resetReaderIndex();
                }
            }
        } catch (Exception e) {
            System.err.println("总体异常: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
