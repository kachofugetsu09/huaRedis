package site.hnfy258.datatype;

public class RedisZset implements  RedisData{
    @Override
    public long timeout() {
        return 0;
    }

    @Override
    public void setTimeout(long timeout) {

    }
}
