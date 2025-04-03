package site.hnfy258.protocal;

import io.netty.buffer.ByteBuf;

public class SimpleString extends Resp
{
    public static final SimpleString OK = new SimpleString("OK");
    private final String content;

    public SimpleString(String content)
    {
        this.content = content;
    }

    public String getContent()
    {
        return content;
    }
    public  void write(Resp resp, ByteBuf buffer)
    {
        buffer.writeByte((byte) '+');
        String content   = ((SimpleString) resp).getContent();
        char[] charArray = content.toCharArray();
        for (char each : charArray)
        {
            buffer.writeByte((byte) each);
        }
        buffer.writeByte((byte) '\r');
        buffer.writeByte((byte) '\n');
    }
}
