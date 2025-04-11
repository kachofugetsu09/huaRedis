package site.hnfy258.aof;

public enum AOFSyncStrategy {
    ALWAYS,    // 每次写入都同步
    EVERYSEC,  // 每秒同步一次
    NO         // 由操作系统决定同步时机
}