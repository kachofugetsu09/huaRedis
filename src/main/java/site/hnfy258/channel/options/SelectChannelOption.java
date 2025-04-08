package site.hnfy258.channel.options;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import site.hnfy258.channel.LocalChannelOption;

import java.util.concurrent.ThreadFactory;

public class SelectChannelOption implements LocalChannelOption {
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private static final int IO_THREADS = Math.min(8, Runtime.getRuntime().availableProcessors() * 2);

    public SelectChannelOption() {
        ThreadFactory bossFactory = new DefaultThreadFactory("redis-boss");
        ThreadFactory workerFactory = new DefaultThreadFactory("redis-io");
        this.bossGroup = new NioEventLoopGroup(1, bossFactory);
        this.workerGroup = new NioEventLoopGroup(IO_THREADS, workerFactory);
    }

    @Override
    public EventLoopGroup boss() {
        return bossGroup;
    }

    @Override
    public EventLoopGroup selectors() {
        return workerGroup;
    }

    @Override
    public Class<?> getChannelClass() {
        return NioServerSocketChannel.class;
    }
}
