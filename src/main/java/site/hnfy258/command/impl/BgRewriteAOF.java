package site.hnfy258.command.impl;

import org.apache.log4j.Logger;
import site.hnfy258.RedisCore;
import site.hnfy258.aof.AOFHandler;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.protocal.Errors;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.SimpleString;

import java.util.concurrent.CompletableFuture;

public class BgRewriteAOF implements Command {
    Logger logger = Logger.getLogger(BgRewriteAOF.class);
    private RedisCore redisCore;

    public BgRewriteAOF(RedisCore redisCore) {
        this.redisCore = redisCore;
    }



    @Override
    public CommandType getType() {
        return CommandType.BGREWRITEAOF;
    }

    @Override
    public void setContext(Resp[] array) {
        if (array.length < 1) {
            throw new IllegalArgumentException("Invalid BGREWRITEAOF command");
        }
    }

    @Override
    public Resp handle() {
        AOFHandler aofHandler = redisCore.getRedisService().getAofHandler();
        if (aofHandler == null) {
            return new Errors("ERR BGREWRITEAOF is not supported");
        }
        CompletableFuture<Boolean> future = aofHandler.startRewrite();
        if(future != null){
            future.whenComplete((success, ex) -> {
                if(ex!= null){
                    logger.error("BGREWRITEAOF执行失败", ex);
                }
                else if(!success){
                    logger.error("BGREWRITEAOF执行失败");
                }
            });

            return new SimpleString("BackGround append only file rewriting started");
        }
        else{
            return new Errors("ERR BGREWRITEAOF is not supported");
        }
    }
}
