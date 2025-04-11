package site.hnfy258.rdb.constants;

public class RDBConstants {
    public static final String RDB_FILE_NAME = "dump.rdb";
    public static final byte RDB_OPCODE_EOF = (byte) 255;
    public static final byte STRING_TYPE = 0;
    public static final byte LIST_TYPE = 1;
    public static final byte SET_TYPE = 2;
    public static final byte ZSET_TYPE = 3;
    public static final byte HASH_TYPE = 4;
    public static final byte RDB_OPCODE_SELECTDB = (byte) 254;
    public static final byte RDB_OPCODE_EXPIRETIME = (byte) 253;
    public static final byte RDB_OPCODE_EXPIRETIME_MS = (byte) 252;
}
