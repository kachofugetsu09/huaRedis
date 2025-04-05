package site.hnfy258.protocal;

import io.netty.buffer.ByteBuf;

public class RespArray extends Resp
{

    Resp[] array;

    public RespArray(Resp[] array)
    {
        this.array = array;
    }

    public Resp[] getArray()
    {
        return array;
    }

    @Override
    public void write(Resp resp, ByteBuf buffer) {
        buffer.writeByte((byte) '*');
        Resp[] array = ((RespArray) resp).getArray();
        String length = String.valueOf(array.length);
        char[] charArray = length.toCharArray();
        for (char each : charArray) {
            buffer.writeByte((byte) each);
        }
        buffer.writeByte((byte) '\r');
        buffer.writeByte((byte) '\n');
        for (Resp each : array) {
            each.write(each, buffer);
        }
    }
}
