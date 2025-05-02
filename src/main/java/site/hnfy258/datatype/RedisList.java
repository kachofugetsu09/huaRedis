package site.hnfy258.datatype;

import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;

import java.util.*;
import java.util.stream.Collectors;

public class RedisList implements  RedisData,Cloneable{
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

    @Override
    public RedisData deepCopy() {
        try {
            RedisList clone = (RedisList) super.clone();
            clone.list.addAll(list);
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
         }

    }

    @Override
    public boolean isImmutable() {
        return false;
    }

    @Override
    public List<Resp> convertToRESP() {
        List<Resp> res = new ArrayList<>();

        for(BytesWrapper value: list){
            res.add(new BulkString(value));
        }
        return res;
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

    public List<BytesWrapper> lrange(int start, int end) {
        int size = list.size();
        start = Math.max(0, start);
        end = Math.min(size - 1, end);

        if (start <= end) {
            return list.subList(start, end + 1);
        }
        return Collections.emptyList();
    }

    public int remove(BytesWrapper value) {
        int count = 0;
        while(list.remove(value)){
            count++;
        }
        return count;
    }

    public List<BytesWrapper> getAllElements() {
        return new ArrayList<>(list);
    }

}
