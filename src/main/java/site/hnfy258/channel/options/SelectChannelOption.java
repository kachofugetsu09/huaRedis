package site.hnfy258.channel.options;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import site.hnfy258.channel.LocalChannelOption;

public class SelectChannelOption implements LocalChannelOption {
    private final NioEventLoopGroup singleEventLoop;

    public SelectChannelOption() {
        this.singleEventLoop = new NioEventLoopGroup(1, r -> {
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
        return NioServerSocketChannel.class;
    }
}
