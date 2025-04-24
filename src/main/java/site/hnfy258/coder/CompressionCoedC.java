package site.hnfy258.coder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageCodec;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.log4j.Logger;

import java.util.List;

public class CompressionCoedC extends ByteToMessageCodec<ByteBuf> {

    private static final Logger logger = Logger.getLogger(CompressionCoedC.class);
    private static final int COMPRESSION_THRESHOLD = 64;
    private final LZ4Compressor compressor;
    private final LZ4FastDecompressor decompressor;

    public CompressionCoedC() {
        this.compressor = LZ4Factory.fastestInstance().fastCompressor();
        this.decompressor = LZ4Factory.fastestInstance().fastDecompressor();
        logger.info("初始化压缩编解码器，压缩阈值: " + COMPRESSION_THRESHOLD + " 字节");
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, ByteBuf msg, ByteBuf out) throws Exception {
        // 保存原始readerIndex
        int readerIndex = msg.readerIndex();
        int originalSize = msg.readableBytes();

        // 只压缩大于阈值的
        if(originalSize > COMPRESSION_THRESHOLD){
            // 1.写入压缩标记
            out.writeByte(1);

            // 2.准备压缩
            byte[] bytes = new byte[originalSize];
            msg.getBytes(readerIndex, bytes); // 使用getBytes而不是readBytes，避免移动readerIndex

            // 3.计算压缩后可能的最大长度
            int maxLength = compressor.maxCompressedLength(originalSize);
            byte[] compressedBytes = new byte[maxLength];

            // 4.压缩数据
            int compressedLength = compressor.compress(bytes, 0, originalSize, compressedBytes, 0, maxLength);

            // 5.写入原始长度
            out.writeInt(originalSize);

            // 6.写入压缩后的长度
            out.writeInt(compressedLength);

            // 7.写入压缩数据
            out.writeBytes(compressedBytes, 0, compressedLength);

            float compressionRatio = (float)compressedLength / originalSize * 100;
            logger.info(String.format("数据已压缩: 原始大小=%d字节, 压缩后=%d字节, 压缩率=%.2f%%",
                    originalSize, compressedLength, compressionRatio));
        } else {
            // 未压缩的情况
            out.writeByte(0);
            out.writeInt(originalSize);
            out.writeBytes(msg, readerIndex, originalSize);

            logger.info("数据未压缩: 大小=" + originalSize + "字节 (小于阈值)");
        }
    }


    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // 检查是否有足够的数据来读取标记
        if (in.readableBytes() < 1) {
            return;
        }

        in.markReaderIndex();

        // 读取压缩标记
        byte isCompressed = in.readByte();

        if (isCompressed == 1) {
            // 需要读取原始长度和压缩长度
            if (in.readableBytes() < 8) {
                in.resetReaderIndex();
                return;
            }

            // 读取原始长度
            int originalLength = in.readInt();

            // 读取压缩长度
            int compressedLength = in.readInt();

            // 确保有足够的数据
            if (in.readableBytes() < compressedLength) {
                in.resetReaderIndex();
                return;
            }

            // 读取压缩数据
            byte[] compressed = new byte[compressedLength];
            in.readBytes(compressed);

            // 解压缩数据
            byte[] decompressed = new byte[originalLength];
            decompressor.decompress(compressed, 0, decompressed, 0, originalLength);

            // 创建解压后的 ByteBuf
            ByteBuf result = ctx.alloc().buffer(originalLength);
            result.writeBytes(decompressed, 0, originalLength);
            out.add(result);

            logger.info(String.format("数据已解压缩: 压缩大小=%d字节, 解压后=%d字节",
                    compressedLength, originalLength));
        } else {
            // 需要读取未压缩长度
            if (in.readableBytes() < 4) {
                in.resetReaderIndex();
                return;
            }

            // 读取未压缩长度
            int length = in.readInt();

            // 确保有足够的数据
            if (in.readableBytes() < length) {
                in.resetReaderIndex();
                return;
            }

            // 创建结果 ByteBuf 并正确读取数据
            ByteBuf result = ctx.alloc().buffer(length);
            in.readBytes(result, length);
            result.writerIndex(length); // 确保写入索引正确设置
            out.add(result);

            logger.info("接收到未压缩数据: 大小=" + length + "字节");
        }
    }
}
