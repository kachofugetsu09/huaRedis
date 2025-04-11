package site.hnfy258.rdb.core;

import site.hnfy258.rdb.constants.RDBConstants;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class RDBUtil {
    public static void writeString(DataOutputStream dos, byte[] str) throws IOException {
        writeLength(dos, str.length);
        dos.write(str);
    }

    public static byte[] readString(DataInputStream dis) throws IOException {
        long length = readLength(dis);
        if (length < 0 || length > Integer.MAX_VALUE) {
            throw new IOException("Invalid string length: " + length);
        }
        byte[] str = new byte[(int) length];
        dis.readFully(str);
        return str;
    }

    public static void writeLength(DataOutputStream dos, long length) throws IOException {
        dos.writeInt((int) length);
    }

    public static long readLength(DataInputStream dis) throws IOException {
        return dis.readInt() & 0xFFFFFFFFL;
    }

    public static void writeRDBHeader(DataOutputStream dos) throws IOException {
        dos.writeBytes("REDIS0001");
    }

    public static void writeSelectDB(DataOutputStream dos, int dbIndex) throws IOException {
        dos.writeByte(RDBConstants.RDB_OPCODE_SELECTDB);
        writeLength(dos, dbIndex);
    }

    public static void writeRDBFooter(DataOutputStream dos) throws IOException {
        dos.writeByte(RDBConstants.RDB_OPCODE_EOF);
        dos.writeLong(0); // CRC64校验和，目前未处理
    }

    public static boolean validateRDBHeader(DataInputStream dis) throws IOException {
        byte[] header = new byte[9];
        int bytesRead = dis.read(header);
        return bytesRead == 9 && "REDIS0001".equals(new String(header, StandardCharsets.UTF_8));
    }
}