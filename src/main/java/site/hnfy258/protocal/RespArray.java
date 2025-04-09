package site.hnfy258.protocal;

import io.netty.buffer.ByteBuf;

public class RespArray extends Resp {
    private static final byte[] CRLF = {'\r', '\n'};
    private static final int MAX_CACHED_SIZE = 32;
    private static final byte[][] CACHED_INT_BYTES = new byte[MAX_CACHED_SIZE][];

    static {
        for (int i = 0; i < MAX_CACHED_SIZE; i++) {
            CACHED_INT_BYTES[i] = Integer.toString(i).getBytes();
        }
    }

    private Resp[] array;

    public RespArray(Resp[] array) {
        this.array = array;
    }

    public Resp[] getArray() {
        return array;
    }

    @Override
    public void write(Resp resp, ByteBuf buffer) {
        buffer.writeByte('*');
        writeIntString(buffer, array.length);
        for (Resp each : array) {
            each.write(each, buffer);
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
}
