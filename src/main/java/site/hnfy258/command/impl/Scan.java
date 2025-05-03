package site.hnfy258.command.impl;

import site.hnfy258.RedisCore;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.protocal.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public class Scan implements Command {
    private final RedisCore redisCore;
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
        
        // 默认值
        this.pattern = null;
        this.count = 10;
        
        for (int i = 2; i < array.length; i += 2) {
            if (i + 1 >= array.length) {
                break; // 没有足够的参数
            }
            
            String option = ((BulkString) array[i]).getContent().toUtf8String().toLowerCase();
            String value = ((BulkString) array[i + 1]).getContent().toUtf8String();
            
            switch (option) {
                case "match":
                    this.pattern = value;
                    break;
                case "count":
                    try {
                        this.count = Long.parseLong(value);
                        // 确保count至少为1
                        if (this.count < 1) {
                            this.count = 10;
                        }
                        // 如果是RDM请求大量键，增加返回数量
                        if (this.count >= 5000) {
                            this.count = Math.min(this.count, 10000); // 最多10000个
                        }
                    } catch (NumberFormatException e) {
                        this.count = 10; // 默认值
                    }
                    break;
            }
        }
    }

    @Override
    public Resp handle() {
        // 确保我们获取当前数据库的键
        int currentDbIndex = redisCore.getCurrentDB().getId();
        
        // 确保使用正确的数据库
        int actualDbIndex = redisCore.getCurrentDBIndex();
        if (actualDbIndex != currentDbIndex) {
            redisCore.selectDB(currentDbIndex);
        }
        
        // 只获取当前数据库的键
        Set<BytesWrapper> keys = redisCore.keys();
        List<BytesWrapper> matchedKeys = new ArrayList<>();
        
        Pattern regexPattern = null;
        if (pattern != null && !pattern.equals("*")) {
            regexPattern = Pattern.compile(patternToRegex(pattern));
        }
        
        long nextCursor = 0;
        int scanned = 0;
        boolean started = cursor == 0 || keys.size() <= cursor;
        
        for (BytesWrapper key : keys) {
            if (!started && scanned < cursor) {
                scanned++;
                continue;
            }
            started = true;
            
            // 处理匹配
            boolean matches = true;
            if (regexPattern != null) {
                String keyStr = key.toUtf8String();
                matches = regexPattern.matcher(keyStr).matches();
            }
            
            if (matches) {
                matchedKeys.add(key);
            }
            
            if (matchedKeys.size() >= count) {
                nextCursor = scanned + 1;
                break;
            }
            scanned++;
        }
        
        // 如果我们处理完了所有键，重置游标
        if (scanned >= keys.size() || nextCursor >= keys.size()) {
            nextCursor = 0;
        }
        
        // 确保SCAN结束后数据库索引没有变化
        int afterScanDbIndex = redisCore.getCurrentDBIndex();
        if (afterScanDbIndex != currentDbIndex) {
            redisCore.selectDB(currentDbIndex);
        }
        
        // 构建结果
        Resp[] result = new Resp[2];
        result[0] = new BulkString(new BytesWrapper(String.valueOf(nextCursor).getBytes()));
        
        Resp[] keyArray = new Resp[matchedKeys.size()];
        for (int i = 0; i < matchedKeys.size(); i++) {
            keyArray[i] = new BulkString(matchedKeys.get(i));
        }
        result[1] = new RespArray(keyArray);
        
        return new RespArray(result);
    }
    
    private String patternToRegex(String pattern) {
        StringBuilder sb = new StringBuilder();
        sb.append("^");
        
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            switch (c) {
                case '*':
                    sb.append(".*");
                    break;
                case '?':
                    sb.append(".");
                    break;
                case '.':
                case '(':
                case ')':
                case '[':
                case ']':
                case '{':
                case '}':
                case '\\':
                case '+':
                case '^':
                case '$':
                case '|':
                    sb.append('\\').append(c);
                    break;
                default:
                    sb.append(c);
            }
        }
        
        sb.append("$");
        return sb.toString();
    }
}
