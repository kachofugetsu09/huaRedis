package site.hnfy258.datatype;

import site.hnfy258.protocal.Resp;

import java.util.List;

public interface RedisData {
    long timeout();
    void setTimeout(long timeout);

    RedisData deepCopy();

    boolean isImmutable();

    List<Resp> convertToRESP();

}
