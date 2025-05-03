// Set.java
package site.hnfy258.command.impl.String;

import site.hnfy258.RedisCore;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.datatype.RedisString;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Errors;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.SimpleString;

public class Set implements Command {
    private final RedisCore redisCore;
    private BytesWrapper key;
    private BytesWrapper value;
    private long timeout = -1;
    private boolean notExistSet = false;
    private boolean existSet = false;

    public Set(RedisCore core) {
        this.redisCore = core;
    }

    @Override
    public CommandType getType() {
        return CommandType.SET;
    }

    @Override
    public void setContext(Resp[] array) {
        // 检查参数数量，SET至少需要key和value两个参数
        if (array.length < 3) {
            throw new IllegalArgumentException("Wrong number of arguments for SET command");
        }
        
        // 检查参数类型
        if (!(array[1] instanceof BulkString) || !(array[2] instanceof BulkString)) {
            throw new IllegalArgumentException("Invalid SET command argument type");
        }
        
        key = ((BulkString) array[1]).getContent();
        value = ((BulkString) array[2]).getContent();
        
        System.out.println("SET命令: key=" + key.toUtf8String() + ", value=" + value.toUtf8String() + 
                          ", 线程=" + Thread.currentThread().getName());
        
        // 处理可选参数
        int index = 3;
        while (index < array.length) {
            if (!(array[index] instanceof BulkString)) {
                throw new IllegalArgumentException("Invalid SET command option format");
            }
            
            String option = ((BulkString) array[index]).getContent().toUtf8String().toUpperCase();
            index++;
            
            // 处理选项参数
            switch (option) {
                case "EX":
                    if (index >= array.length || !(array[index] instanceof BulkString)) {
                        throw new IllegalArgumentException("EX option requires a numeric argument");
                    }
                    String second = ((BulkString) array[index]).getContent().toUtf8String();
                    try {
                        timeout = Long.parseLong(second) * 1000;
                        index++;
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("EX option value must be integer");
                    }
                    break;
                case "PX":
                    if (index >= array.length || !(array[index] instanceof BulkString)) {
                        throw new IllegalArgumentException("PX option requires a numeric argument");
                    }
                    String millisecond = ((BulkString) array[index]).getContent().toUtf8String();
                    try {
                        timeout = Long.parseLong(millisecond);
                        index++;
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("PX option value must be integer");
                    }
                    break;
                case "NX":
                    notExistSet = true;
                    break;
                case "XX":
                    existSet = true;
                    break;
                default:
                    throw new IllegalArgumentException("Invalid SET option: " + option);
            }
        }
        
        // NX和XX选项不能同时使用
        if (notExistSet && existSet) {
            throw new IllegalArgumentException("NX and XX options are mutually exclusive");
        }
    }

    @Override
    public Resp handle() {
        try {
            // 获取当前数据库ID
            int currentDbIndex = redisCore.getCurrentDB().getId();
            System.out.println("执行SET命令: key=" + key.toUtf8String() + ", 数据库=" + currentDbIndex + 
                              ", 线程=" + Thread.currentThread().getName());
            
            if (notExistSet && redisCore.exist(key)) {
                System.out.println("SET命令(NX选项): 键已存在，操作取消");
                return BulkString.NullBulkString;
            } else if (existSet && !redisCore.exist(key)) {
                System.out.println("SET命令(XX选项): 键不存在，操作取消");
                return BulkString.NullBulkString;
            } else {
                if (timeout != -1) {
                    timeout += System.currentTimeMillis();
                    System.out.println("SET命令: 设置超时时间 " + timeout);
                }
                
                RedisString stringData = new RedisString(value);
                stringData.setTimeout(timeout);
                
                // 明确输出在哪个数据库中设置键
                System.out.println("SET命令: 在数据库 " + currentDbIndex + " 中设置键 " + 
                                  key.toUtf8String() + " = " + value.toUtf8String());
                
                redisCore.put(key, stringData);
                return new SimpleString("OK");
            }
        } catch (Exception e) {
            System.err.println("SET命令执行错误: " + e.getMessage());
            e.printStackTrace();
            return new Errors("ERR " + e.getMessage());
        }
    }
}
