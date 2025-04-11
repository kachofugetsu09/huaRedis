package site.hnfy258.aof.writer;

import org.apache.log4j.Logger;
import site.hnfy258.aof.AOFSyncStrategy;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class AOFWriter implements Writer {
    private static final Logger logger = Logger.getLogger(AOFWriter.class);
    
    private final RandomAccessFile raf;
    private final FileChannel fileChannel;
    private final AOFSyncStrategy syncStrategy;
    
    public AOFWriter(String filename, AOFSyncStrategy syncStrategy) throws IOException {
        this.raf = new RandomAccessFile(filename, "rw");
        this.fileChannel = raf.getChannel();
        this.syncStrategy = syncStrategy;
        raf.seek(raf.length());
    }
    
    @Override
    public void write(ByteBuffer buffer) throws IOException {
        if (buffer != null && buffer.hasRemaining()) {
            fileChannel.write(buffer);
            if (syncStrategy != AOFSyncStrategy.NO) {
                fileChannel.force(false);
            }
        }
    }
    
    @Override
    public void close() {
        try {
            if (fileChannel != null && fileChannel.isOpen()) {
                fileChannel.close();
            }
            if (raf != null) {
                raf.close();
            }
        } catch (IOException e) {
            logger.error("Error closing AOF resources", e);
        }
    }
    
    @Override
    public void force() throws IOException {
        if (fileChannel != null && fileChannel.isOpen()) {
            fileChannel.force(false);
        }
    }
}