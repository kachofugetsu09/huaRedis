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
                // 标记当前读取位置，以便在解码失败时重置
                in.markReaderIndex();

                // 检查是否有足够的数据可读
                if (in.readableBytes() < 4) { // 至少需要一个类型字符和最小的数据
                    // 数据不完整，等待更多数据
                    return;
                }

                try {
                    // 尝试解码
                    Resp decoded = Resp.decode(in);
                    if (decoded != null) {
                        logger.debug("解码成功: " + decoded);
                        out.add(decoded);
                    }
                } catch (IllegalStateException e) {
                    // 命令不完整，重置读取位置并等待更多数据
                    if (e.getMessage().contains("没有读取到完整的命令")) {
                        logger.debug("命令不完整，等待更多数据");
                        in.resetReaderIndex();
                        return;
                    }
                    // 其他解码异常
                    logger.error("解码异常: " + e.getMessage());
                    in.resetReaderIndex();
                } catch (Exception e) {
                    // 其他异常
                    logger.error("解码过程中发生异常: " + e.getMessage());
                    in.resetReaderIndex();
                }
            }
        } catch (Exception e) {
            logger.error("总体异常: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
