package site.hnfy258.datatype;

import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class BytesWrapper {
    static final Charset CHARSET = StandardCharsets.UTF_8;
    private final byte[] bytes;

    public BytesWrapper(byte[] bytes) {
        this.bytes = bytes;
    }
    public byte[] getBytes() {
        return bytes;
    }

    @Override
    public boolean equals(Object obj) {
        if(this == obj){
            return true;
        }
        if(obj == null|| obj.getClass() != getClass()){
            return false;
        }
        BytesWrapper other = (BytesWrapper) obj;
        return Arrays.equals(bytes, other.bytes);
    }



    public String toUtf8String() {
        return new String(bytes, CHARSET);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

}
