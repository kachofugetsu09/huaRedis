package site.hnfy258.datatype;

import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class BytesWrapper  implements Comparable<BytesWrapper> {
    public static final Charset CHARSET = StandardCharsets.UTF_8;
    private byte[] bytes;

    public BytesWrapper(byte[] bytes) {
        this.bytes = bytes;
    }

    public byte[] getBytes() {
        return bytes;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || obj.getClass() != getClass()) {
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

    @Override
    public int compareTo(BytesWrapper o) {
        int minLength = Math.min(bytes.length, o.bytes.length);
        for (int i = 0; i < minLength; i++) {
            int cmp = Byte.compare(bytes[i], o.bytes[i]);
            if (cmp != 0) {
                return cmp;
            }
        }
        return bytes.length - o.bytes.length;
    }

    @Override
    public BytesWrapper clone() {
        try {
            BytesWrapper clone = (BytesWrapper) super.clone();
            clone.bytes = Arrays.copyOf(this.bytes, this.bytes.length);
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }

    public BytesWrapper deepCopy() {
        return this.clone();
    }
}
