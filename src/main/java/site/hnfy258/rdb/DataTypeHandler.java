package site.hnfy258.rdb;

import site.hnfy258.RedisCore;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.datatype.RedisData;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public interface DataTypeHandler {
    void save(DataOutputStream dos, BytesWrapper key, RedisData value) throws IOException;
    void load(DataInputStream dis, int currentDb, RedisCore redisCore) throws IOException;
}