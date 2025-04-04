package site.hnfy258.coder;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import site.hnfy258.protocal.Resp;

public class MyResponseEncoder extends MessageToByteEncoder<Resp> {
    @Override
    protected void encode(ChannelHandlerContext ctx, Resp resp, ByteBuf out) throws Exception {
        try {
            resp.write(resp, out);
            System.out.println("Encoded response: " + ByteBufUtil.hexDump(out));
        } catch(Exception e) {
            e.printStackTrace();
            ctx.close();
        }
    }
}