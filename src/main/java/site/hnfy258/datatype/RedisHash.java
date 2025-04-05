package site.hnfy258.datatype;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedisHash implements  RedisData{
    private volatile long timeout = -1;
    private final  Map<BytesWrapper,BytesWrapper> RedishHashMap = new HashMap<>();
    @Override
    public long timeout() {
        return timeout;
    }


    @Override
    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }
    
    
    public int put(BytesWrapper field,BytesWrapper value){
        return RedishHashMap.put(field,value)==null?1:0;
    }

    public Map<BytesWrapper,BytesWrapper> getMap() {
        return RedishHashMap;
    }

    public int del(List<BytesWrapper> fields){
        return (int)fields.stream().
                filter(key-> RedishHashMap.remove(key)!=null).count();
    }
}
