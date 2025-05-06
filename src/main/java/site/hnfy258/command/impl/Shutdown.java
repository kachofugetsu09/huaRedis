package site.hnfy258.command.impl;

import site.hnfy258.RedisCore;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.SimpleString;

/**
 * SHUTDOWN命令的实现
 * 用于优雅地关闭Redis服务
 */
public class Shutdown implements Command {
    private final RedisCore redisCore;
    
    public Shutdown(RedisCore core) {
        this.redisCore = core;
    }
    
    @Override
    public CommandType getType() {
        return CommandType.SHUTDOWN;
    }
    
    @Override
    public void setContext(Resp[] array) {
        // 无需处理参数
    }
    
    @Override
    public Resp handle() {
        // 返回响应后立即启动关闭过程
        new Thread(() -> {
            try {
                // 给客户端一点时间接收响应
                Thread.sleep(500);
                // 关闭服务
                if (redisCore.getRedisService() != null) {
                    // 只关闭当前节点的服务而不触发System.exit()
                    redisCore.getRedisService().shutdownSingleNode();
                }
            } catch (Exception e) {
                System.err.println("关闭服务出错: " + e.getMessage());
            }
        }).start();
        
        return new SimpleString("OK - Shutting down only this server node");
    }
} 