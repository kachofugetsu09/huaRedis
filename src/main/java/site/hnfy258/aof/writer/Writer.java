package site.hnfy258.aof.writer;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface Writer {
    void write(ByteBuffer buffer) throws IOException;

    void close();

    void force() throws IOException;
}
