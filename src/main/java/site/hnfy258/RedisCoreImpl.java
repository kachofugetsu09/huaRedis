package site.hnfy258;

import io.netty.channel.Channel;
import org.apache.log4j.Logger;
import site.hnfy258.database.RedisDB;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.datatype.RedisData;
import site.hnfy258.rdb.core.RDBHandler;
import site.hnfy258.server.MyRedisService;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class RedisCoreImpl implements RedisCore {
    private static final Logger logger = Logger.getLogger(RedisCoreImpl.class);
    
    private final List<RedisDB> databases;
    private final int dbNum;
    // 每个节点使用一个整数表示当前数据库索引，不再使用线程映射
    private volatile int currentDbIndex = 0;
    
    private final ConcurrentHashMap<BytesWrapper, Channel> clients;
    private final Map<Channel, BytesWrapper> clientNames;
    private RDBHandler rdbHandler;

    private final MyRedisService redisService;


    public RedisCoreImpl(int dbNum, MyRedisService redisService) {
        this.dbNum = dbNum;
        this.databases = new ArrayList<>(dbNum);
        for (int i = 0; i < dbNum; i++) {
            databases.add(new RedisDB(i));
        }
        this.clients = new ConcurrentHashMap<>();
        this.clientNames = new ConcurrentHashMap<>();
        this.redisService = redisService;
        logger.info("Redis核心初始化完成: 数据库数量=" + dbNum + ", 初始数据库索引=0");
    }
    
    @Override
    public RedisDB getCurrentDB() {
        // 直接使用currentDbIndex，不再依赖于线程
        return databases.get(currentDbIndex);
    }

    public void selectDB(int index) {
        if (index >= 0 && index < dbNum) {
            // 记录数据库切换
            int oldIndex = currentDbIndex;
            // 简单地更新整个节点的数据库索引
            currentDbIndex = index;
            // 仅当索引发生变化时记录日志
            if (oldIndex != index) {
                String threadName = Thread.currentThread().getName();
                logger.info("数据库索引切换: " + oldIndex + " -> " + index + 
                          ", 线程=" + threadName + 
                          ", 节点ID=" + (redisService != null && redisService.getCurrentNode() != null ? 
                                       redisService.getCurrentNode().getId() : "unknown"));
            }
        } else {
            logger.error("尝试切换到无效的数据库索引: " + index + ", 当前索引=" + currentDbIndex);
            throw new IllegalArgumentException("Invalid DB index: " + index);
        }
    }

    @Override
    public Set<BytesWrapper> keys() {
        // 添加调试输出
        int dbIndex = currentDbIndex;
        RedisDB db = databases.get(dbIndex);
        Set<BytesWrapper> keys = db.keys();
        if (logger.isDebugEnabled()) {
            logger.debug("获取键列表: 数据库=" + dbIndex + ", 键数量=" + keys.size());
        }
        return keys;
    }

    @Override
    public void putClient(BytesWrapper connectionName, Channel channelContext) {
        clients.put(connectionName, channelContext);
        clientNames.put(channelContext, connectionName);
    }

    @Override
    public boolean exist(BytesWrapper key) {
        boolean exists = getCurrentDB().exist(key);
        if (logger.isDebugEnabled()) {
            logger.debug("检查键是否存在: key=" + key.toUtf8String() + 
                      ", 数据库=" + currentDbIndex + 
                      ", 结果=" + exists);
        }
        return exists;
    }

    @Override
    public void put(BytesWrapper key, RedisData redisData) {
        // 确保是在正确的数据库中操作
        RedisDB db = databases.get(currentDbIndex);
        String keyStr = key.toUtf8String();
        boolean isUpdate = db.exist(key);
        String valuePreview = redisData != null ? 
                            redisData.getClass().getSimpleName() : "null";
        
        logger.info((isUpdate ? "更新" : "新增") + "键值: key=" + keyStr + 
                  ", 数据库=" + currentDbIndex + 
                  ", 值类型=" + valuePreview + 
                  ", 线程=" + Thread.currentThread().getName());
        
        db.put(key, redisData);
    }

    @Override
    public RedisData get(BytesWrapper key) {
        // 确保是在正确的数据库中操作
        RedisDB db = databases.get(currentDbIndex);
        RedisData data = db.get(key);
        
        String keyStr = key.toUtf8String();
        String valuePreview = data != null ? 
                            data.getClass().getSimpleName() : "null";
        
        logger.info("读取键值: key=" + keyStr + 
                  ", 数据库=" + currentDbIndex + 
                  ", 结果=" + (data != null ? "找到" : "未找到") + 
                  ", 值类型=" + valuePreview + 
                  ", 线程=" + Thread.currentThread().getName());
        
        return data;
    }

    @Override
    public long remove(List<BytesWrapper> keys) {
        long count = getCurrentDB().remove(keys);
        if (logger.isDebugEnabled() && !keys.isEmpty()) {
            logger.debug("删除键: count=" + count + 
                      ", 数据库=" + currentDbIndex + 
                      ", 第一个键=" + keys.get(0).toUtf8String());
        }
        return count;
    }

    @Override
    public void cleanAll() {
        logger.info("清空所有数据库");
    }

    public void clear() {
        logger.info("清空当前数据库: index=" + currentDbIndex);
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
    public void setDB(int dbIndex, BytesWrapper bytesWrapper, RedisData redisData) {
        logger.info("直接设置数据库数据: dbIndex=" + dbIndex + 
                  ", key=" + bytesWrapper.toUtf8String() + 
                  ", 值类型=" + (redisData != null ? redisData.getClass().getSimpleName() : "null"));
        databases.get(dbIndex).put(bytesWrapper, redisData);
    }

    public void disconnectClient(Channel channel) {
        BytesWrapper connectionName = clientNames.remove(channel);
        if (connectionName != null) {
            clients.remove(connectionName);
        }
    }

    public Map<BytesWrapper, RedisData> getDBData(int dbIndex) {
        Map<BytesWrapper, RedisData> data = databases.get(dbIndex).getAll();
        if (logger.isDebugEnabled()) {
            logger.debug("获取数据库数据: dbIndex=" + dbIndex + ", 键数量=" + data.size());
        }
        return data;
    }

    @Override
    public MyRedisService getRedisService() {
        return redisService;
    }


    public RDBHandler getRDBHandler() {
        return rdbHandler;
    }

    public void setRDBHandler(RDBHandler rdbHandler) {
        this.rdbHandler = rdbHandler;
    }
    
    // 获取当前数据库索引
    public int getCurrentDBIndex() {
        return currentDbIndex;
    }
}