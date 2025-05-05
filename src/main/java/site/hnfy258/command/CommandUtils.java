package site.hnfy258.command;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

/**
 * 命令工具类，用于集中处理命令类型判断
 */
public class CommandUtils {

    // 写命令集合
    private static final Set<CommandType> WRITE_COMMANDS = EnumSet.of(
            // 字符串操作
            CommandType.SET, CommandType.MSET, CommandType.INCR,

            // 键管理
            CommandType.DEL, CommandType.EXPIRE,

            // 哈希表操作
            CommandType.HSET, CommandType.HMEST, CommandType.HDEL,

            // 列表操作
            CommandType.LPUSH, CommandType.RPUSH, CommandType.LPOP, CommandType.RPOP, CommandType.LREM,

            // 集合操作
            CommandType.SADD, CommandType.SREM, CommandType.SPOP,

            // 有序集合操作
            CommandType.ZADD, CommandType.ZREM,

            // 数据库操作
            CommandType.SELECT, CommandType.SAVE,
            
            // 系统操作
            CommandType.SHUTDOWN
    );

    // 静默命令（不需要记录日志的命令）
    private static final Set<String> SILENT_COMMANDS = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList("SCAN", "PING", "INFO"))
    );

    // 不需要复制的命令
    private static final Set<String> NON_REPLICATE_COMMANDS = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList("SCAN", "PING", "INFO", "SLAVEOF", "SHUTDOWN"))
    );

    /**
     * 判断命令类型是否是写命令
     * @param commandType 命令类型
     * @return 如果是写命令返回true
     */
    public static boolean isWriteCommand(CommandType commandType) {
        return WRITE_COMMANDS.contains(commandType);
    }

    /**
     * 判断命令名称是否是写命令
     * @param commandName 命令名称
     * @return 如果是写命令返回true
     */
    public static boolean isWriteCommand(String commandName) {
        try {
            CommandType commandType = CommandType.valueOf(commandName);
            return isWriteCommand(commandType);
        } catch (IllegalArgumentException e) {
            return false; // 未知命令视为非写命令
        }
    }

    /**
     * 判断命令是否是静默命令（不需要记录日志）
     * @param commandName 命令名称
     * @return 如果是静默命令返回true
     */
    public static boolean isSilentCommand(String commandName) {
        return SILENT_COMMANDS.contains(commandName);
    }

    /**
     * 判断命令是否需要复制到从节点
     * @param commandName 命令名称
     * @return 如果不需要复制返回true
     */
    public static boolean isNonReplicateCommand(String commandName) {
        return NON_REPLICATE_COMMANDS.contains(commandName);
    }
}