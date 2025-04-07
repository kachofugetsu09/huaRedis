package site.hnfy258.coder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import site.hnfy258.protocal.Resp;
import org.apache.log4j.*;

import java.util.List;

public class MyDecoder extends ByteToMessageDecoder {
    Logger logger = Logger.getLogger(MyDecoder.class);
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        try {
            if (in.readableBytes() > 0) {
                logger.info("开始解码");
                // 标记当前读取位置
                in.markReaderIndex();
                try {
                    Resp decoded = Resp.decode(in);
                    logger.info("解码成功");
                    out.add(decoded);
                } catch (Exception e) {
                    logger.error("解码异常: " + e.getMessage());
                    in.resetReaderIndex();
                }
            }
        } catch (Exception e) {
            logger.error("总体异常: " + e.getMessage());
        }
    }
}
