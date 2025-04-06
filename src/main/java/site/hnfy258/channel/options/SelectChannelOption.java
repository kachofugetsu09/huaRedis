package site.hnfy258.channel.options;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import site.hnfy258.channel.LocalChannelOption;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class SelectChannelOption implements LocalChannelOption {
    private final NioEventLoopGroup boss;
    private final NioEventLoopGroup selectors;

    public SelectChannelOption(NioEventLoopGroup boss, NioEventLoopGroup selectors) {
        this.boss = boss;
        this.selectors = selectors;
    }
    public SelectChannelOption()
    {
        this.boss = new NioEventLoopGroup(4, new ThreadFactory() {
            private AtomicInteger index = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "Server_boss_" + index.getAndIncrement());
            }
        });

        this.selectors = new NioEventLoopGroup(8, new ThreadFactory() {
            private AtomicInteger index = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "Server_selector_" + index.getAndIncrement());
            }
        });
    }
    @Override
    public EventLoopGroup boss() {
        return  this.boss;
    }

    @Override
    public EventLoopGroup selectors() {
        return  this.selectors;
    }

    @Override
    public Class getChannelClass() {
        return NioServerSocketChannel.class;
    }
}

