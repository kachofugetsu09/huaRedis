package site.hnfy258;

 import io.netty.channel.Channel;
 import site.hnfy258.database.RedisDB;
 import site.hnfy258.datatype.BytesWrapper;
 import site.hnfy258.datatype.RedisData;

 import java.util.List;
 import java.util.Map;
 import java.util.Set;
 
 public interface RedisCore
 {
     Set<BytesWrapper> keys();
 
     void putClient(BytesWrapper connectionName, Channel channelContext);
 
     boolean exist(BytesWrapper key);
 
     void put(BytesWrapper key, RedisData redisData);
 
     RedisData get(BytesWrapper key);
 
     long remove(List<BytesWrapper> keys);
 
     void cleanAll();

     RedisDB getCurrentDB();

     void selectDB(int index);

     int getDbNum();

     Map<BytesWrapper, RedisData> getAll();

     void setDB(int currentDb, BytesWrapper bytesWrapper, RedisData redisData);

     Map<BytesWrapper, RedisData> getDBData(int dbIndex);

 }