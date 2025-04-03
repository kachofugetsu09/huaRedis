package site.hnfy258.protocal;


import io.netty.buffer.ByteBuf;

public class Errors extends Resp
{
    String content;

    public Errors(String content)
    {
        this.content = content;
    }

    public String getContent()
    {
        return content;
    }

    @Override
    public void write(Resp resp, ByteBuf buffer) {
        buffer.writeByte((byte) '-');
        String content   = ((Errors) resp).getContent();
        char[] charArray = content.toCharArray();
        for (char each : charArray)
        {
            buffer.writeByte((byte) each);
        }
        buffer.writeByte((byte) '\r');
        buffer.writeByte((byte) '\n');
    }
}
