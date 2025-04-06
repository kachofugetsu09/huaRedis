package site.hnfy258.datatype;

import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class RedisList implements  RedisData{
    private volatile long timeout;
    private final LinkedList<BytesWrapper> list = new LinkedList<>();


    public RedisList() {
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

    public List<BytesWrapper> lrange(int start, int end){
        List<BytesWrapper> res =  list.stream().skip(start)
                .limit(end-start>=0?end-start+1:0).collect(Collectors.toList());
        return res;
    }

    public int remove(BytesWrapper value) {
        int count = 0;
        while(list.remove(value)){
            count++;
        }
        return count;
    }
}
