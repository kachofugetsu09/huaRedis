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
import java.util.ArrayList;
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

    public boolean rewrite(){
        if(!isRewriting.compareAndSet(false,true)){
            logger.error("正在重写");
            return false;
        }

        try{
            File tempFile = new File(tempFilename);
            if(tempFile.exists()){
                tempFile.delete();
            }

            boolean success = doWrite();

            if(success){
                tempFile.renameTo(new File(aofFilename));
            }else{
                logger.error("重写失败");
                tempFile.delete();
            }
            return success;
        }catch(Exception e){
            logger.error("Error during AOFRewriter", e);
        }finally {
            isRewriting.set(false);
        }
        return false;
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
