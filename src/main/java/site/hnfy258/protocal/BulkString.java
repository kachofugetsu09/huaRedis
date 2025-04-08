package site.hnfy258.protocal;

import io.netty.buffer.ByteBuf;
import site.hnfy258.datatype.BytesWrapper;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;

import java.nio.charset.StandardCharsets;

public class BulkString extends Resp {
    public static final BulkString NullBulkString = new BulkString(null);
    BytesWrapper content;

    private static final GenericObjectPool<BulkString> POOL = new GenericObjectPool<>(new BasePooledObjectFactory<BulkString>() {
        @Override
        public BulkString create() {
            return new BulkString(null);
        }

        @Override
        public PooledObject<BulkString> wrap(BulkString bulkString) {
            return new DefaultPooledObject<>(bulkString);
        }
    });


    public static BulkString newInstance(BytesWrapper content) {
        try {
            BulkString instance = POOL.borrowObject();
            instance.content = content;
            return instance;
        } catch (Exception e) {
            // 如果池出现问题，退回到直接创建新对象
            return new BulkString(content);
        }
    }

    public void recycle() {
        content = null;
        POOL.returnObject(this);
    }

    public BulkString(BytesWrapper content) {
        this.content = content;
    }

    public BytesWrapper getContent() {
        return content;
    }

    @Override
    public void write(Resp resp, ByteBuf buffer) {
        buffer.writeByte('$');
        BytesWrapper content = ((BulkString) resp).getContent();
        if (content == null) {
            buffer.writeBytes(new byte[]{'-', '1', '\r', '\n'});
        } else {
            int length = content.getBytes().length;
            if (length == 0) {
                buffer.writeBytes(new byte[]{'0', '\r', '\n', '\r', '\n'});
            } else {
                writeIntString(buffer, length);
                buffer.writeBytes(content.getBytes());
                buffer.writeBytes(new byte[]{'\r', '\n'});
            }
        }
    }

    private void writeIntString(ByteBuf buffer, int value) {
        if (value < 10) {
            buffer.writeByte((byte) ('0' + value));
        } else {
            buffer.writeBytes(String.valueOf(value).getBytes(StandardCharsets.US_ASCII));
        }
        buffer.writeBytes(new byte[]{'\r', '\n'});
    }
}