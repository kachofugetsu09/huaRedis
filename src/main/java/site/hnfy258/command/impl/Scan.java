package site.hnfy258.command.impl;

import site.hnfy258.RedisCore;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.protocal.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class Scan implements Command {
    private RedisCore redisCore;
    private long cursor;
    private String pattern;
    private long count;

    public Scan(RedisCore redisCore) {
        this.redisCore = redisCore;
    }

    @Override
    public CommandType getType() {
        return CommandType.SCAN;
    }

    @Override
    public void setContext(Resp[] array) {
        if (array.length < 2) {
            throw new IllegalArgumentException("SCAN command requires at least a cursor");
        }
        this.cursor = Long.parseLong(((BulkString) array[1]).getContent().toUtf8String());
        
        for (int i = 2; i < array.length; i += 2) {
            String option = ((BulkString) array[i]).getContent().toUtf8String().toLowerCase();
            if (i + 1 >= array.length) {
                throw new IllegalArgumentException("SCAN option " + option + " requires an argument");
            }
            String value = ((BulkString) array[i + 1]).getContent().toUtf8String();
            switch (option) {
                case "match":
                    this.pattern = value;
                    break;
                case "count":
                    this.count = Long.parseLong(value);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported SCAN option: " + option);
            }
        }
        
        if (this.count <= 0) {
            this.count = 10; // Default count
        }
    }

    @Override
    public Resp handle() {
        Set<BytesWrapper> keys = redisCore.keys();
        List<BytesWrapper> matchedKeys = new ArrayList<>();
        
        long nextCursor = 0;
        int scanned = 0;
        boolean started = false;
        
        for (BytesWrapper key : keys) {
            if (!started && scanned < cursor) {
                scanned++;
                continue;
            }
            started = true;
            
            if (pattern == null || key.toUtf8String().matches(patternToRegex(pattern))) {
                matchedKeys.add(key);
            }
            
            if (matchedKeys.size() >= count) {
                nextCursor = scanned + 1;
                break;
            }
            scanned++;
        }
        
        if (nextCursor >= keys.size()) {
            nextCursor = 0; // We've reached the end, reset cursor
        }
        
        Resp[] result = new Resp[2];
        result[0] = new BulkString(new BytesWrapper((String.valueOf(nextCursor)).getBytes()));
        
        Resp[] keyArray = new Resp[matchedKeys.size()];
        for (int i = 0; i < matchedKeys.size(); i++) {
            keyArray[i] = new BulkString(matchedKeys.get(i));
        }
        result[1] = new RespArray(keyArray);
        
        return new RespArray(result);
    }
    
    private String patternToRegex(String pattern) {
        return pattern.replace("*", ".*").replace("?", ".");
    }
}
