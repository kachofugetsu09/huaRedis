package site.hnfy258.command;

import site.hnfy258.protocal.Resp;

public interface Command {
    CommandType getType();
    void setContext(Resp[] array);
    Resp handle(); // 返回Resp对象而不是直接写入channel
}