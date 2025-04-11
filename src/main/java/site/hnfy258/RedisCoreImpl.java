package site.hnfy258;

import io.netty.channel.Channel;
import site.hnfy258.database.RedisDB;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.datatype.RedisData;
import site.hnfy258.datatype.RedisString;
import site.hnfy258.rdb.RDBHandler;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class RedisCoreImpl implements RedisCore {
    private final List<RedisDB> databases;
    private final int dbNum;
    private final ThreadLocal<Integer> currentDB;
    private final ConcurrentHashMap<BytesWrapper, Channel> clients;
    private final Map<Channel, BytesWrapper> clientNames;
    private RDBHandler rdbHandler;

    public RedisCoreImpl(int dbNum) {
        this.dbNum = dbNum;
        this.databases = new ArrayList<>(dbNum);
        for (int i = 0; i < dbNum; i++) {
            databases.add(new RedisDB(i));
        }
        this.currentDB = ThreadLocal.withInitial(() -> 0);
        this.clients = new ConcurrentHashMap<>();
        this.clientNames = new ConcurrentHashMap<>();
    }
    @Override
    public RedisDB getCurrentDB() {
        return databases.get(currentDB.get());
    }

    public void selectDB(int index) {
        if (index >= 0 && index < dbNum) {
            currentDB.set(index);
        } else {
            throw new IllegalArgumentException("Invalid DB index");
        }
    }

    @Override
    public Set<BytesWrapper> keys() {
        return getCurrentDB().keys();
    }

    @Override
    public void putClient(BytesWrapper connectionName, Channel channelContext) {
        clients.put(connectionName, channelContext);
        clientNames.put(channelContext, connectionName);
    }

    @Override
    public boolean exist(BytesWrapper key) {
        return getCurrentDB().exist(key);
    }

    @Override
    public void put(BytesWrapper key, RedisData redisData) {
        getCurrentDB().put(key, redisData);
    }

    @Override
    public RedisData get(BytesWrapper key) {
        return getCurrentDB().get(key);
    }

    @Override
    public long remove(List<BytesWrapper> keys) {
        return getCurrentDB().remove(keys);
    }

    @Override
    public void cleanAll() {

    }

    public void clear() {
        getCurrentDB().clear();
    }

    public int size() {
        return getCurrentDB().size();
    }

    public Channel getClient(BytesWrapper connectionName) {
        return clients.get(connectionName);
    }

    public BytesWrapper getClientName(Channel channelContext) {
        return clientNames.get(channelContext);
    }

    public void removeClient(Channel channelContext) {
        BytesWrapper connectionName = clientNames.remove(channelContext);
        if (connectionName != null) {
            clients.remove(connectionName);
        }
    }

    public int getDbNum() {
        return dbNum;
    }

    @Override
    public Map<BytesWrapper, RedisData> getAll() {
        Map<BytesWrapper, RedisData> map = new HashMap<>();
        for (RedisDB redisDB : databases) {
            for (BytesWrapper key : redisDB.keys()) {
                RedisData redisData = redisDB.get(key);
                if (redisData != null) {
                    map.put(new BytesWrapper(key.getBytes()), redisData);
                }
            }
        }
        return map;
    }

    @Override
    public void setDB(int currentDb, BytesWrapper bytesWrapper, RedisData redisData) {
        databases.get(currentDb).put(bytesWrapper, redisData);
    }

    public void disconnectClient(Channel channel) {
        BytesWrapper connectionName = clientNames.remove(channel);
        if (connectionName != null) {
            clients.remove(connectionName);
        }
    }

    public Map<BytesWrapper, RedisData> getDBData(int dbIndex) {
        return databases.get(dbIndex).getAll();
    }


    public RDBHandler getRDBHandler() {
        return rdbHandler;
    }

    public void setRDBHandler(RDBHandler rdbHandler) {
        this.rdbHandler = rdbHandler;
    }
}