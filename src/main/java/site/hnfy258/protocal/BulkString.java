package site.hnfy258.protocal;


import io.netty.buffer.ByteBuf;
import site.hnfy258.datatype.BytesWrapper;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class BulkString extends Resp {
    public static final BulkString NullBulkString = new BulkString(null);
    static final Charset CHARSET = StandardCharsets.UTF_8;
    BytesWrapper content;

    public BulkString(BytesWrapper content) {
        this.content = content;
    }

    public BytesWrapper getContent() {
        return content;
    }


    @Override
    public void write(Resp resp, ByteBuf buffer) {
        buffer.writeByte((byte) '$');
        BytesWrapper content = ((BulkString) resp).getContent();
        if (content == null)
        {
            buffer.writeByte((byte) '-');
            buffer.writeByte((byte) '1');
            buffer.writeByte((byte) '\r');
            buffer.writeByte((byte) '\n');
        }
        else if (content.getBytes().length == 0)
        {
            buffer.writeByte((byte) '0');
            buffer.writeByte((byte) '\r');
            buffer.writeByte((byte) '\n');
            buffer.writeByte((byte) '\r');
            buffer.writeByte((byte) '\n');
    }
        else{
            String length = String.valueOf(content.getBytes().length);
            char[] charArray = length.toCharArray();
            for (char each : charArray)
            {
                buffer.writeByte((byte) each);
            }
            buffer.writeByte((byte) '\r');
            buffer.writeByte((byte) '\n');
            buffer.writeBytes(content.getBytes());
            buffer.writeByte((byte) '\r');
            buffer.writeByte((byte) '\n');
        }
        }
}

