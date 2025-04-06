package site.hnfy258;

import io.netty.channel.Channel;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.datatype.RedisData;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class RedisCoreImpl implements RedisCore
 {
     private final ConcurrentHashMap<BytesWrapper, RedisData> map         = new ConcurrentHashMap<>();
     private final ConcurrentHashMap<BytesWrapper, Channel> clients     = new ConcurrentHashMap<>();
     private final Map<Channel, BytesWrapper> clientNames = new ConcurrentHashMap<>();
 
     @Override
     public Set<BytesWrapper> keys()
     {
         return map.keySet();
     }
 
     @Override
     public void putClient(BytesWrapper connectionName, Channel channelContext)
     {
         clients.put(connectionName, channelContext);
         clientNames.put(channelContext, connectionName);
     }
 
     @Override
     public boolean exist(BytesWrapper key)
     {
         return map.containsKey(key);
     }
 
     @Override
     public void put(BytesWrapper key, RedisData redisData)
     {
         map.put(key, redisData);
     }
 
     @Override
     public RedisData get(BytesWrapper key)
     {
         RedisData redisData = map.get(key);
         if (redisData == null)
         {
             return null;
         }
         if (redisData.timeout() == -1)
         {
             return redisData;
         }
         if (redisData.timeout() < System.currentTimeMillis())
         {
             map.remove(key);
             return null;
         }
         return redisData;
     }
 
     @Override
     public long remove(List<BytesWrapper> keys)
     {
         return keys.stream().peek(map::remove).count();
     }
 
     @Override
     public void cleanAll()
     {
         map.clear();
     }

     public void broadcastToAllClients(String message) {
         for (Channel channel : clients.values()) {
             channel.writeAndFlush(message);
         }
     }

     public void disconnectClient(Channel channel) {
         BytesWrapper clientName = clientNames.remove(channel);
         if (clientName != null) {
             clients.remove(clientName);
         }
         channel.close();
     }

     public int getConnectedClientsCount() {
         return clients.size();
     }

 }