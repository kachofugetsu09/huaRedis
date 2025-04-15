package site.hnfy258.datatype;

import site.hnfy258.utils.SDS;

public class RedisString implements RedisData, Cloneable {
    private volatile long timeout;
    private SDS value;

    public RedisString(BytesWrapper value) {
        this.value = new SDS(value.getBytes());
        this.timeout = -1;
    }

    public RedisString(SDS value) {
        this.value = value;
        this.timeout = -1;
    }

    @Override
    public long timeout() {
        return timeout;
    }

    @Override
    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    @Override
    public RedisData deepCopy() {
        try {
            RedisString clone = (RedisString) super.clone();
            clone.value = value.deepCopy();
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isImmutable() {
        return true;
    }

    public BytesWrapper getValue() {
        return new BytesWrapper(value.getBytes());
    }

    public SDS getSdsValue() {
        return value;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setValue(BytesWrapper value) {
        this.value = new SDS(value.getBytes());
    }

    public void setSdsValue(SDS value) {
        this.value = value;
    }

    public long incr() {
        try {
            long currentValue = Long.parseLong(value.toString());
            long newValue = currentValue + 1;
            this.value = new SDS(String.valueOf(newValue).getBytes());
            return newValue;
        } catch (NumberFormatException e) {
            throw new IllegalStateException("value is not an integer or out of range");
        }
    }
}
