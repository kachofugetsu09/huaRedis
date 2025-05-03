package site.hnfy258.aof.rewriter;



import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.log4j.Logger;
import site.hnfy258.RedisCore;
import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.datatype.RedisData;
import site.hnfy258.protocal.BulkString;
import site.hnfy258.protocal.Resp;
import site.hnfy258.protocal.RespArray;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
public class AOFRewriter {
    private static final Logger logger = Logger.getLogger(AOFRewriter.class);

    //核心
    private final RedisCore redisCore;
    //临时文件名
    private final String tempFilename;
    //AOF文件名
    private final String aofFilename;
    //是否在重写流程
    private final AtomicBoolean isRewriting;
    //重写的buffer大小
    private int bufferSize;


    public AOFRewriter(RedisCore redisCore, String aofFilename, int bufferSize) {
        this.redisCore = redisCore;
        this.aofFilename = aofFilename;
        this.tempFilename = aofFilename+".tmp";
        this.isRewriting = new AtomicBoolean(false);
        this.bufferSize = bufferSize;
    }

    public boolean canRewrite(){
        return !isRewriting.get();
    }

    List<ByteBuffer> rewriteBuffer;

    public boolean rewrite(){
        if(!isRewriting.compareAndSet(false,true)){
            logger.error("正在重写");
            return false;
        }

        try{
            rewriteBuffer = Collections.synchronizedList(new ArrayList<>());

            redisCore.getRedisService().getAofHandler().startRewriteBuffer();

            File tempFile = new File(tempFilename);
            if(tempFile.exists()){
                tempFile.delete();
            }



            boolean success = doWrite();


            if(success){
                List<ByteBuffer> buffers = redisCore.getRedisService().getAofHandler().stopRewriteBufferAndGet();

                // 将重写期间的命令追加到新AOF文件
                appendRewriteBufferToTempFile(buffers);

                // 原子性地替换文件
                atomicReplaceFile(tempFile, new File(aofFilename));
            }else{
                logger.error("重写失败");
                tempFile.delete();

                redisCore.getRedisService().getAofHandler().discardRewriteBuffer();
            }
            return success;
        }catch(Exception e){
            logger.error("Error during AOFRewriter", e);
            redisCore.getRedisService().getAofHandler().discardRewriteBuffer();
            return false;
        }finally {
            isRewriting.set(false);
        }
    }

    private void atomicReplaceFile(File tempFile, File targetFile) throws IOException {
        // 尝试直接重命名（在大多数系统上是原子操作）
        if (tempFile.renameTo(targetFile)) {
            return;
        }

        // 如果直接重命名失败，使用备份策略
        File backupFile = new File(targetFile.getAbsolutePath() + ".bak");

        // 删除已存在的备份文件
        if (backupFile.exists()) {
            if (!backupFile.delete()) {
                logger.warn("无法删除备份文件: " + backupFile.getAbsolutePath());
            }
        }

        // 如果目标文件存在，先尝试删除它
        if (targetFile.exists()) {
            if (!targetFile.delete()) {
                logger.warn("无法删除目标文件: " + targetFile.getAbsolutePath());
                // 如果无法删除，尝试重命名为备份
                if (!targetFile.renameTo(backupFile)) {
                    logger.warn("无法创建备份文件，将使用复制方式替换");
                    // 如果重命名也失败，则尝试使用REPLACE_EXISTING选项
                    Files.copy(tempFile.toPath(), targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
                    tempFile.delete();
                    return;
                }
            }
        }

        // 复制临时文件到目标位置
        try {
            Files.copy(tempFile.toPath(), targetFile.toPath());
            tempFile.delete();
        } catch (IOException e) {
            logger.error("复制文件失败", e);
            // 如果复制失败，尝试使用REPLACE_EXISTING选项
            Files.copy(tempFile.toPath(), targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            tempFile.delete();
        }

        // 操作成功后删除备份
        if (backupFile.exists()) {
            if (!backupFile.delete()) {
                logger.warn("无法删除备份文件: " + backupFile.getAbsolutePath());
            }
        }
    }


    private void appendRewriteBufferToTempFile(List<ByteBuffer> buffers) throws IOException{
        if(buffers==null || buffers.isEmpty()){
            return;
        }
        try(RandomAccessFile raf = new RandomAccessFile(tempFilename,"rw");
        FileChannel fileChannel = raf.getChannel();){
            fileChannel.position(fileChannel.size());

            for(ByteBuffer buffer : buffers){
                if(buffer.hasRemaining() && buffer!=null){
                    fileChannel.write(buffer);
                }
            }
            fileChannel.force(false);
        }

    }


    private boolean doWrite() {
        try(RandomAccessFile raf = new RandomAccessFile(tempFilename,"rw");
            FileChannel fileChannel = raf.getChannel();
        ){
            ByteBuf byteBuf = Unpooled.buffer();

            for(int i=0;i<redisCore.getDbNum();i++){

                Map<BytesWrapper, RedisData> dbData = redisCore.getDBData(i);
                if(!dbData.isEmpty()){
                    writeSelectCommand(byteBuf,i);
                    flushIfNeeded(byteBuf,fileChannel);
                }
                for(Map.Entry<BytesWrapper, RedisData> entry : dbData.entrySet()){
                    BytesWrapper key = entry.getKey();
                    RedisData value = entry.getValue();

                    List<Resp> commandList = value.convertToRESP();
                    for(Resp command : commandList){
                        command.write(command, byteBuf);
                        flushIfNeeded(byteBuf,fileChannel);
                    }
                }
            }
            if(byteBuf.readableBytes()>0){
                ByteBuffer  byteBuffer = byteBuf.nioBuffer();
                fileChannel.write(byteBuffer);
            }

        }catch(Exception e){
            logger.error("Error during AOFRewriter", e);
        }
        return true;
    }

    private void writeSelectCommand(ByteBuf byteBuf, int i) {
        List<Resp> selectCommand = new ArrayList<>();
        selectCommand.add(new BulkString(new BytesWrapper("SELECT".getBytes())));
        selectCommand.add(new BulkString(new BytesWrapper(String.valueOf(i).getBytes())));

        RespArray selectCommandArray = new RespArray(selectCommand.toArray(new Resp[0]));
        selectCommandArray.write(selectCommandArray, byteBuf);

    }

    private void flushIfNeeded(ByteBuf buffer, FileChannel channel) throws IOException {
        // 当缓冲区使用超过75%时刷新
        if (buffer.readableBytes() > (bufferSize * 0.75)) {
            ByteBuffer byteBuffer = buffer.nioBuffer();
            channel.write(byteBuffer);
            buffer.clear();
        }
    }


}
