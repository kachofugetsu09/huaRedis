package site.hnfy258.datatype;

public class RedisString implements  RedisData{
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
}
