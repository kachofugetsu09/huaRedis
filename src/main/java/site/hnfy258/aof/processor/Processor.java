package site.hnfy258.aof.processor;

import site.hnfy258.protocal.Resp;

import java.io.IOException;

public interface Processor {
    void append(Resp command);


    void flush() throws IOException;

    void stop();

    boolean isRunning();
}
