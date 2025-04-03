package site.hnfy258.protocal;

import io.netty.buffer.ByteBuf;

public class RespInt extends Resp
{
    int value;

    public RespInt(int value)
    {
        this.value = value;
    }

    public int getValue()
    {
        return value;
    }

    @Override
    public void write(Resp resp, ByteBuf buffer) {
        buffer.writeByte((byte) ':');
        String content   = String.valueOf(((RespInt) resp).getValue());
        char[] charArray = content.toCharArray();
        for (char each : charArray)
        {
            buffer.writeByte((byte) each);
        }
        buffer.writeByte((byte) '\r');
        buffer.writeByte((byte) '\n');
    }
}
