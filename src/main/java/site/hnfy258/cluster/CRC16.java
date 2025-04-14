package site.hnfy258.cluster;

public class CRC16 {
    private static final int[] LOOKUP_TABLE = new int[256];

    static {
        for (int i = 0; i < 256; i++) {
            int crc = i;
            for (int j = 0; j < 8; j++) {
                if ((crc & 1) == 1) {
                    crc = (crc >>> 1) ^ 0xA001;
                } else {
                    crc = crc >>> 1;
                }
            }
            LOOKUP_TABLE[i] = crc;
        }
    }

    public static int crc16(byte[] bytes) {
        int crc = 0;
        for (byte b : bytes) {
            crc = (crc >>> 8) ^ LOOKUP_TABLE[(crc ^ b) & 0xFF];
        }
        return crc;
    }
}
