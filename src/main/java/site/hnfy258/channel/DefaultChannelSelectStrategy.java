package site.hnfy258.channel;


import io.netty.channel.epoll.Epoll;
import io.netty.channel.kqueue.KQueue;
import site.hnfy258.channel.options.EpollChannelOption;
import site.hnfy258.channel.options.KqueueChannelOption;
import site.hnfy258.channel.options.SelectChannelOption;

public class DefaultChannelSelectStrategy implements ChannelSelectStrategy{
    @Override
    public LocalChannelOption select() {
        if(KQueue.isAvailable()){
            return new KqueueChannelOption();
        }
        if(Epoll.isAvailable()){
            return new EpollChannelOption();
        }
        return new SelectChannelOption();
    }
}
