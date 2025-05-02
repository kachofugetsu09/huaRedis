package site.hnfy258.command.impl;

import site.hnfy258.RedisCore;
import site.hnfy258.aof.AOFHandler;
import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.protocal.Errors;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.SimpleString;

public class BgRewriteAOF implements Command {
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
        return aofHandler.startRewrite()
                ? new SimpleString("OK")
                : new Errors("ERR BGREWRITEAOF already in progress");
    }
}
