// Get.java
package site.hnfy258.command.impl.String;

import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.datatype.RedisData;
import site.hnfy258.datatype.RedisString;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Errors;
import site.hnfy258.protocal.Resp;
import site.hnfy258.RedisCore;

public class Get implements Command {
    private BytesWrapper key;
    private final RedisCore redisCore;

    public Get(RedisCore redisCore) {
        this.redisCore = redisCore;
    }

    @Override
    public CommandType getType() {
        return CommandType.GET;
    }

    @Override
    public void setContext(Resp[] array) {
        // 严格检查参数数量
        if (array.length != 2) {
            throw new IllegalArgumentException("Wrong number of arguments for GET command");
        }
        
        if (!(array[1] instanceof BulkString)) {
            throw new IllegalArgumentException("Invalid GET command argument type");
        }
        
        this.key = ((BulkString) array[1]).getContent();
        System.out.println("GET命令: key=" + key.toUtf8String() + ", 线程=" + Thread.currentThread().getName());
    }

    @Override
    public Resp handle() {
        try {
            // 获取当前数据库ID
            int currentDbIndex = redisCore.getCurrentDB().getId();
            System.out.println("执行GET命令: key=" + key.toUtf8String() + ", 数据库=" + currentDbIndex + 
                              ", 线程=" + Thread.currentThread().getName());
            
            // 检查键是否存在
            if (!redisCore.exist(key)) {
                System.out.println("GET命令: 键 " + key.toUtf8String() + " 在数据库 " + currentDbIndex + " 中不存在");
                return BulkString.NullBulkString;
            }
            
            // 尝试获取值
            RedisData data = redisCore.get(key);
            if (data == null) {
                System.out.println("GET命令: 键 " + key.toUtf8String() + " 的值为null");
                return BulkString.NullBulkString;
            }
            
            // 检查类型
            if (!(data instanceof RedisString)) {
                System.out.println("GET命令: 键 " + key.toUtf8String() + " 的类型不是字符串");
                return new Errors("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
            
            // 获取字符串值并返回
            RedisString redisString = (RedisString) data;
            BytesWrapper value = redisString.getValue();
            System.out.println("GET命令: 成功从数据库 " + currentDbIndex + " 获取键 " + 
                              key.toUtf8String() + " 的值 = " + value.toUtf8String());
            
            return new BulkString(value);
        } catch (ClassCastException e) {
            // 如果键存在但不是字符串类型
            System.err.println("GET命令类型错误: " + e.getMessage());
            return new Errors("WRONGTYPE Operation against a key holding the wrong kind of value");
        } catch (Exception e) {
            System.err.println("GET命令执行错误: " + e.getMessage());
            e.printStackTrace();
            return new Errors("ERR " + e.getMessage());
        }
    }
}
