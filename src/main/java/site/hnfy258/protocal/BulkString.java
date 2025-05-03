package site.hnfy258.protocal;

import io.netty.buffer.ByteBuf;
import site.hnfy258.datatype.BytesWrapper;

public class BulkString extends Resp {
    private static final byte[] NULL_BYTES = {'-', '1', '\r', '\n'};
    private static final byte[] EMPTY_BYTES = {'0', '\r', '\n', '\r', '\n'};
    private static final byte[] CRLF = {'\r', '\n'};
    private static final int MAX_CACHED_SIZE = 32;
    private static final byte[][] CACHED_INT_BYTES = new byte[MAX_CACHED_SIZE][];

    static {
        for (int i = 0; i < MAX_CACHED_SIZE; i++) {
            CACHED_INT_BYTES[i] = Integer.toString(i).getBytes();
        }
    }

    public static final BulkString NullBulkString = new BulkString((BytesWrapper) null);

    private final BytesWrapper content;

    public BulkString(BytesWrapper content) {
        this.content = content;
    }

    public BulkString(String val) {
        this.content = new BytesWrapper(val.getBytes());
    }

    public BytesWrapper getContent() {
        return content;
    }

    @Override
    public void write(Resp resp, ByteBuf buffer) {
        buffer.writeByte('$');
        if (content == null) {
            buffer.writeBytes(NULL_BYTES);
        } else {
            int length = content.getBytes().length;
            if (length == 0) {
                buffer.writeBytes(EMPTY_BYTES);
            } else {
                writeIntString(buffer, length);
                buffer.writeBytes(content.getBytes());
                buffer.writeBytes(CRLF);
            }
        }
    }

    private void writeIntString(ByteBuf buffer, int value) {
        if (value < MAX_CACHED_SIZE) {
            buffer.writeBytes(CACHED_INT_BYTES[value]);
        } else {
            buffer.writeBytes(Integer.toString(value).getBytes());
        }
        buffer.writeBytes(CRLF);
    }

    public boolean isNull() {
        return content == null;
    }
}
