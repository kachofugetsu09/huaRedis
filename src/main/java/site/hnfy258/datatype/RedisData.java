package site.hnfy258.datatype;

public interface RedisData {
    long timeout();
    void setTimeout(long timeout);

    RedisData deepCopy();

    boolean isImmutable();

}
