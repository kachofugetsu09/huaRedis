package site.hnfy258.sentinel;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import site.hnfy258.cluster.ClusterClient;
import site.hnfy258.cluster.ClusterNode;
import site.hnfy258.coder.MyDecoder;
import site.hnfy258.coder.MyResponseEncoder;
import site.hnfy258.command.impl.Ping;
import site.hnfy258.protocal.RespArray;
import site.hnfy258.protocal.SimpleString;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class Sentinel {

    private String host;
    private int port;

    List<ClusterClient> clusterClients;
    List<Sentinel> neighborSentinels;
    ScheduledExecutorService  normalScheduler;
    ScheduledExecutorService extremeScheduler;

    private Map<String, ClusterNode> allNodes;
    private Map<String, ClusterNode> masterNodes;

    private static final int DOWN_AFTER_MILLS = 3000;
    private static final int NORMAL_PING_INTERVAL = 1000;
    private static final int EXTREM_PING_INTERVAL = 100;

    List<ClusterClient> SdownedClients;

    private Channel channel;

    public Sentinel(String host, int port) {
        this.host = host;
        this.port = port;
        this.SdownedClients = new ArrayList<>();
    }

    public List<ClusterNode> getSlaveOfMaster(String masterId){
        List<ClusterNode> slaves = new ArrayList<>();
        for(ClusterNode node:allNodes.values()){
            if(node.getMasterId().equals(masterId)){
                slaves.add(node);
            }
        }
        return slaves;
    }


    public ClusterNode getMasterOfNode(ClusterNode node){
        if(!node.isMaster()){
            return allNodes.get(node.getMasterId());
        }

        return null;
    }

    public void addNeighborSentinel(Sentinel sentinel){
        neighborSentinels.add(sentinel);
    }

    public void addClusterClient(ClusterClient clusterClient){
        clusterClients.add(clusterClient);
        clusterClient.addSentinel(this);
        clusterClient.connect();
        this.connect();
    }

    private void connect() {
    }

    public void start(){
        Bootstrap bootstrap = new Bootstrap();
        NioEventLoopGroup group = new NioEventLoopGroup();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new MyDecoder());
                        pipeline.addLast(new MyResponseEncoder());
                    }
                });

        bootstrap.bind(host,port).addListener(future -> {
            if(future.isSuccess()){
                System.out.println("sentinel start");
            }
            else{
                System.out.println("sentinel start error");
            }
        });

        normalScheduler = Executors.newSingleThreadScheduledExecutor();
        extremeScheduler = Executors.newSingleThreadScheduledExecutor();

        startNormalPing();
        startExtremePing();
    }

    private void startExtremePing() {
        extremeScheduler.scheduleAtFixedRate(()->{
            for(ClusterClient clusterClient:clusterClients){
                if(!clusterClient.isActive() &&
                        SdownedClients.contains(clusterClient)){
                    clusterClient.sendMessage(new SimpleString("PING"));
                }
            }
        }, 0,EXTREM_PING_INTERVAL, TimeUnit.MILLISECONDS);
    }

    private void startNormalPing() {
        normalScheduler.scheduleAtFixedRate(()->{
            for(ClusterClient clusterClient:clusterClients){
                if(clusterClient.isActive() && !
                        SdownedClients.contains(clusterClient)){
                    clusterClient.sendMessage(new SimpleString("PING"));
                }
            }
        }, 0,NORMAL_PING_INTERVAL, TimeUnit.MILLISECONDS);
    }


    private void notifyOtherSentinels(){

    }

    public  List<ClusterClient> getSdownedClients(){
        return SdownedClients;
    }

    public void sendMessage(RespArray message){

    }




}
