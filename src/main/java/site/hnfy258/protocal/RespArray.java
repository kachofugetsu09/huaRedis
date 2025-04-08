package site.hnfy258.protocal;

import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;

public class RespArray extends Resp {
    private Resp[] array;

    private static final GenericObjectPool<RespArray> POOL = new GenericObjectPool<>(new BasePooledObjectFactory<RespArray>() {
        @Override
        public RespArray create() {
            return new RespArray(null);
        }

        @Override
        public PooledObject<RespArray> wrap(RespArray respArray) {
            return new DefaultPooledObject<>(respArray);
        }
    });

    public RespArray(Resp[] array) {
        this.array = array;
    }

    public static RespArray newInstance(Resp[] array) {
        try {
            RespArray instance = POOL.borrowObject();
            instance.array = array;
            return instance;
        } catch (Exception e) {
            // 如果池出现问题，退回到直接创建新对象
            return new RespArray(array);
        }
    }

    public void recycle() {
        array = null;
        POOL.returnObject(this);
    }

    @Override
    public void write(Resp resp, ByteBuf buffer) {
        buffer.writeByte('*');
        Resp[] array = ((RespArray) resp).getArray();
        writeIntString(buffer, array.length);
        for (Resp each : array) {
            each.write(each, buffer);
        }
    }

    public Resp[] getArray() {
        return array;
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