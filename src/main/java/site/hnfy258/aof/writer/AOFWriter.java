package site.hnfy258.aof.writer;

import org.apache.log4j.Logger;
import site.hnfy258.aof.AOFSyncStrategy;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * AOF文件写入器，负责将数据写入AOF文件
 */
public class AOFWriter implements Writer {
    private static final Logger logger = Logger.getLogger(AOFWriter.class);

    private RandomAccessFile raf;         // 随机访问文件
    private FileChannel fileChannel;      // 文件通道
    private final AOFSyncStrategy syncStrategy; // 同步策略
    private final String filename;        // 文件名

    /**
     * 构造AOF写入器
     * @param filename AOF文件名
     * @param syncStrategy 同步策略
     * @throws IOException 如果文件操作出错
     */
    public AOFWriter(String filename, AOFSyncStrategy syncStrategy) throws IOException {
        // 1. 初始化文件访问对象
        this.filename = filename;
        this.raf = new RandomAccessFile(filename, "rw");
        this.fileChannel = raf.getChannel();
        this.syncStrategy = syncStrategy;

        // 2. 将文件指针移动到文件末尾，以追加方式写入
        raf.seek(raf.length());
    }

    @Override
    public void write(ByteBuffer buffer) throws IOException {
        if (buffer != null && buffer.hasRemaining()) {
            // 1. 将缓冲区数据写入文件通道
            fileChannel.write(buffer);

            // 2. 根据同步策略决定是否立即刷新到磁盘
            if (syncStrategy != AOFSyncStrategy.NO) {
                fileChannel.force(false);
            }
        }
    }

    @Override
    public void close() {
        // 分层关闭资源
        try {
            // 1. 先尝试刷新未写入的数据
            if (fileChannel != null && fileChannel.isOpen()) {
                try {
                    fileChannel.force(true);
                } catch (IOException e) {
                    logger.warn("关闭前刷新AOF文件数据时出错", e);
                }
            }
            
            // 2. 关闭文件通道
            if (fileChannel != null && fileChannel.isOpen()) {
                fileChannel.close();
                fileChannel = null;
            }
            
            // 3. 关闭随机访问文件
            if (raf != null) {
                raf.close();
                raf = null;
            }
        } catch (IOException e) {
            logger.error("关闭AOF资源时出错", e);
        }
    }

    @Override
    public void force() throws IOException {
        // 1. 强制将文件通道中的数据刷新到磁盘
        if (fileChannel != null && fileChannel.isOpen()) {
            fileChannel.force(false);
        }
    }

    @Override
    public void start() {
        try {
            // 如果之前的资源还没关闭，先关闭它们
            close();
            
            // 重新打开文件
            logger.info("重新打开AOF文件: " + filename);
            raf = new RandomAccessFile(filename, "rw");
            
            // 将文件指针移动到文件末尾，以追加方式写入
            raf.seek(raf.length());
            fileChannel = raf.getChannel();
            logger.info("AOF文件已重新打开，当前大小: " + raf.length() + " 字节");
        } catch (IOException e) {
            logger.error("启动AOF写入器时出错: " + e.getMessage(), e);
        }
    }
}
