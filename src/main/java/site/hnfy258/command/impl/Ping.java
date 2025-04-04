package site.hnfy258.command.impl;

import site.hnfy258.command.Command;
import site.hnfy258.command.CommandType;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;

public class Ping implements Command {
    @Override
    public CommandType getType() {
        return CommandType.PING;
    }

    @Override
    public void setContext(Resp[] array) {
    }

    @Override
    public Resp handle() {
        return new BulkString(new BytesWrapper("PONG".getBytes()));
    }
}