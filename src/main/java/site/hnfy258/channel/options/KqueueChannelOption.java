package site.hnfy258.channel.options;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueServerSocketChannel;
import site.hnfy258.channel.LocalChannelOption;

public class KqueueChannelOption implements LocalChannelOption {
    private final KQueueEventLoopGroup singleEventLoop;

    public KqueueChannelOption() {
        this.singleEventLoop = new KQueueEventLoopGroup(1, r -> {
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
        return this.singleEventLoop;
    }

    @Override
    public Class getChannelClass() {
        return KQueueServerSocketChannel.class;
    }
}
