package site.hnfy258;

import io.netty.channel.Channel;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.datatype.RedisData;
import site.hnfy258.storage.OptimizedRedisHashSlots;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class RedisCoreImpl implements RedisCore {
    private final OptimizedRedisHashSlots storage;
    private final ConcurrentHashMap<BytesWrapper, Channel> clients = new ConcurrentHashMap<>();
    private final Map<Channel, BytesWrapper> clientNames = new ConcurrentHashMap<>();

    public RedisCoreImpl() {
        this.storage = new OptimizedRedisHashSlots();
    }

    @Override
    public void put(BytesWrapper key, RedisData redisData) {
        storage.put(key, redisData);
    }

    @Override
    public RedisData get(BytesWrapper key) {
        return storage.get(key);
    }

    @Override
    public boolean exist(BytesWrapper key) {
        return storage.containsKey(key);
    }

    @Override
    public long remove(List<BytesWrapper> keys) {
        return storage.removeAll(keys);
    }

    @Override
    public Set<BytesWrapper> keys() {
        return storage.keySet();
    }

    @Override
    public void putClient(BytesWrapper connectionName, Channel channelContext) {
        clients.put(connectionName, channelContext);
        clientNames.put(channelContext, connectionName);
    }

    @Override
    public void cleanAll() {
        storage.clear();
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