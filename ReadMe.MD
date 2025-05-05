# HuaRedis - 高性能Java实现的Redis服务器

## 项目概述
HuaRedis是一个基于Java和Netty实现的高性能Redis服务器，提供了Redis核心数据结构和命令的实现，并支持AOF和RDB持久化。项目特点是高性能、高并发，性能可达原版Redis的80%-90%。

## 核心特性

### 1. 数据结构支持
- **字符串(String)** - 完整支持SET、GET、INCR等操作
- **列表(List)** - 支持LPUSH、RPUSH、LPOP、RPOP、LRANGE等操作
- **哈希表(Hash)** - 支持HSET、HGET、HMGET、HGETALL等操作
- **集合(Set)** - 支持SADD、SMEMBERS、SISMEMBER、SCARD、SPOP等操作
- **有序集合(SortedSet)** - 支持ZADD、ZRANGE、ZREVRANGE、ZRANGEBYSCORE等操作

### 2. 网络与协议
- **RESP协议支持** - 完整实现Redis序列化协议
- **基于Netty的高性能网络框架** - 利用事件驱动模型提供高并发支持
- **连接池优化** - 高效管理连接资源

### 3. 持久化机制

#### RDB持久化
- **全量快照** - 支持SAVE和BGSAVE命令
- **增量快照** - 仅记录被修改的键值对，提高性能
- **自动触发备份** - 根据配置自动执行持久化
- **文件原子替换** - 确保数据安全

#### AOF持久化
- **三种同步策略** - 支持ALWAYS、EVERYSEC和NO三种刷盘策略
- **零拷贝双缓冲** - 使用DoubleBufferBlockingQueue提高性能
- **RESP格式记录** - 使用标准协议格式记录操作命令
- **动态缓冲区调整** - 根据负载自动调整缓冲区大小

### 4. 集群架构
- **主从复制** - 实现基础的主从复制机制
- **分片集群** - 支持基于固定哈希槽位的数据分片


## 性能优化

### 双缓冲队列 (DoubleBufferBlockingQueue)
专为AOF持久化设计的高性能队列，提供了相比JDK队列更高的性能：

- **零拷贝技术** - 通过缓冲区交换避免内存拷贝
- **动态调整策略** - 根据负载自动扩缩容
- **小数据批处理** - 减少锁竞争提高吞吐量
- **大数据优化** - 对大命令进行分片处理，解决内存问题

### 性能测试结果
与JDK原生队列相比：

1. **标准场景**：吞吐量提升14%，延迟降低13%
2. **大数据场景**：吞吐量提升10%，延迟降低10%
3. **高并发场景**：吞吐量提升5%，延迟降低5%
4. **高频刷盘场景**：轻微性能优势

### 其他优化
- 使用ConcurrentHashMap替代CRC16分片优化查询性能
- 批处理I/O操作减少系统调用
- 高并发下的日志输出频率优化

## 命令支持

### 字符串操作
- `SET key value [EX seconds] [PX milliseconds] [NX|XX]` - 设置键值对
- `GET key` - 获取值
- `DEL key` - 删除键值对
- `EXISTS key` - 检查键是否存在
- `EXPIRE key seconds` - 设置过期时间
- `TTL key` - 获取剩余过期时间
- `INCR key` - 将整数值加一
- `MSET key value [key value ...]` - 批量设置

### 集合操作
- `SADD key member [member ...]` - 添加成员
- `SMEMBERS key` - 获取所有成员
- `SISMEMBER key member` - 判断成员是否存在
- `SCARD key` - 获取成员数量
- `SREM key member [member ...]` - 移除成员
- `SPOP key [count]` - 随机弹出成员

### 哈希表操作
- `HSET key field value [field value ...]` - 设置字段值
- `HGET key field` - 获取字段值
- `HMSET key field value [field value ...]` - 批量设置字段
- `HMGET key field [field ...]` - 批量获取字段
- `HGETALL key` - 获取所有字段和值
- `HDEL key field [field ...]` - 删除字段
- `HEXISTS key field` - 判断字段是否存在
- `HLEN key` - 获取字段数量

### 列表操作
- `LPUSH key element [element ...]` - 左侧插入元素
- `RPUSH key element [element ...]` - 右侧插入元素
- `LPOP key` - 左侧弹出元素
- `RPOP key` - 右侧弹出元素
- `LLEN key` - 获取列表长度
- `LRANGE key start stop` - 获取指定范围元素
- `LREM key count value` - 移除匹配元素

### 有序集合操作
- `ZADD key score member [score member ...]` - 添加成员
- `ZRANGE key start stop [WITHSCORES]` - 按索引范围获取成员
- `ZREVRANGE key start stop [WITHSCORES]` - 倒序获取成员
- `ZRANGEBYSCORE key min max [WITHSCORES]` - 按分数范围获取成员
- `ZREM key member [member ...]` - 移除成员
- `ZCARD key` - 获取成员数量
- `ZSCORE key member` - 获取成员分数

## 项目状态
- [x] Redis核心数据结构实现
- [x] RESP协议支持
- [x] AOF持久化（优化版）
- [x] RDB持久化（增量与全量）
- [x] 分片集群支持
- [x] 主从复制（基础功能）
- [ ] 高可用实现


![HuaRedis.png](HuaRedis.png)



