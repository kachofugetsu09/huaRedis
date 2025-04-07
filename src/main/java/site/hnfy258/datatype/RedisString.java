package site.hnfy258.datatype;

public class RedisString implements RedisData {
    private volatile long timeout;
    private BytesWrapper value;

    public RedisString(BytesWrapper value) {
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

    public BytesWrapper getValue() {
        return value;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setValue(BytesWrapper value) {
        this.value = value;
    }

    public long incr() {
        try {
            long currentValue = Long.parseLong(value.toUtf8String());
            long newValue = currentValue + 1;
            this.value = new BytesWrapper(String.valueOf(newValue).getBytes());
            return newValue;
        } catch (NumberFormatException e) {
            throw new IllegalStateException("value is not an integer or out of range");
        }
    }
}
