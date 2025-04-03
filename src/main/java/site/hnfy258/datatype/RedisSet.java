package site.hnfy258.datatype;

public class RedisSet implements  RedisData{
    @Override
    public long timeout() {
        return 0;
    }

    @Override
    public void setTimeout(long timeout) {

    }
}
