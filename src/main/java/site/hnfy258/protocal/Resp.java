package site.hnfy258.protocal;

import io.netty.buffer.ByteBuf;
import site.hnfy258.datatype.BytesWrapper;

public abstract class Resp {

    public abstract void write(Resp resp, ByteBuf buffer);

    public static Resp decode(ByteBuf buffer){
        char c = (char) buffer.readByte();
        switch (c){
            case '+': return new SimpleString(getString(buffer));
            case '-': return new Errors(getString(buffer));
            case ':': return new RespInt(getNumber(buffer));
            case '$': {
                int length = getNumber(buffer);
                if(buffer.readableBytes()<length+2){
                    throw new IllegalStateException("没有读取到完整的命令");
                }
                byte[] content;
                if(length==-1){
                    content = null;
                }
                else
                {
                    content = new byte[length];
                    buffer.readBytes(content);
                }
                if (buffer.readByte() != '\r' || buffer.readByte() != '\n')
                {
                    throw new IllegalStateException("没有读取到完整的命令");
                }
                return new BulkString(new BytesWrapper(content));
            }
            case '*':{
                int number = getNumber(buffer);
                Resp[] array = new Resp[number];
                for (int i = 0; i < number; i++)
                {
                    array[i] = decode(buffer);
                }
                return new RespArray(array);
            }
            default:
                throw new IllegalStateException("没有读取到完整的命令");
        }
    }

    static int getNumber(ByteBuf buffer)
    {
        char t;
        t = (char) buffer.readByte();
        boolean positive = true;
        int     value    = 0;
        if (t == '-')
        {
            positive = false;
        }
        else
        {
            value = t - '0';
        }
        while (buffer.readableBytes() > 0 && (t = (char) buffer.readByte()) != '\r')
        {
            value = value * 10 + (t - '0');
        }
        if (buffer.readableBytes() == 0 || buffer.readByte() != '\n')
        {
            throw new IllegalStateException("没有读取到完整的命令");
        }
        if (positive == false)
        {
            value = -value;
        }
        return value;
    }

    static String getString(ByteBuf buffer)
    {
        char          c;
        StringBuilder builder = new StringBuilder();
        while (buffer.readableBytes() > 0 && (c = (char) buffer.readByte()) != '\r')
        {
            builder.append(c);
        }
        if (buffer.readableBytes() == 0 || buffer.readByte() != '\n')
        {
            throw new IllegalStateException("没有读取到完整的命令");
        }
        return builder.toString();
    }
}
