package site.hnfy258.channel.options;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import site.hnfy258.channel.LocalChannelOption;

import java.util.concurrent.ThreadFactory;

public class EpollChannelOption implements LocalChannelOption {
    private final EpollEventLoopGroup singleEventLoop;

    public EpollChannelOption() {
        this.singleEventLoop = new EpollEventLoopGroup(1, r -> {
            Thread t = new Thread(r, "Redis-EventLoop");
            t.setDaemon(false);
            return t;
        });
    }

    @Override
    public EventLoopGroup boss() {
        return this.singleEventLoop;
    }

    @Override
    public EventLoopGroup selectors() {
        return this.singleEventLoop; // 使用同一个事件循环组
    }

    @Override
    public Class getChannelClass() {
        return EpollServerSocketChannel.class;
    }
}