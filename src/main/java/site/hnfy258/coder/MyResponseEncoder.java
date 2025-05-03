package site.hnfy258.coder;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.apache.log4j.Logger;
import site.hnfy258.protocal.Resp;

public class MyResponseEncoder extends MessageToByteEncoder<Resp> {
    Logger logger = Logger.getLogger(MyResponseEncoder.class);
    @Override
    protected void encode(ChannelHandlerContext ctx, Resp resp, ByteBuf out) throws Exception {
        try {
            resp.write(resp, out);
            ////logger.info("Encoded response: " + ByteBufUtil.hexDump(out));
        } catch(Exception e) {
            logger.error("编码异常: " + e.getMessage());
            ctx.fireExceptionCaught(e);
            ctx.close();
        }
    }
}