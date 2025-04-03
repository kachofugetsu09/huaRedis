package site.hnfy258.datatype;

import java.util.Deque;
import java.util.LinkedList;

public class RedisList implements  RedisData{
    private volatile long timeout;
    private final Deque<BytesWrapper> list = new LinkedList<>();


    public RedisList() {
        this.timeout = -1;
    }
    @Override
    public long timeout() {
        return 0;
    }

    @Override
    public void setTimeout(long timeout) {

    }

    public long getTimeout() {
        return timeout;
    }

    public int size(){
        return list.size();
    }

    public void lpush(BytesWrapper... values){
        for(BytesWrapper value: values){
            list.addFirst(value);
        }
    }
    public void rpush(BytesWrapper... values){
        for(BytesWrapper value: values){
            list.addLast(value);
        }
    }

    public BytesWrapper lpop(){
        return list.pollFirst();
    }

    public BytesWrapper rpop(){
        return list.pollLast();
    }
}
