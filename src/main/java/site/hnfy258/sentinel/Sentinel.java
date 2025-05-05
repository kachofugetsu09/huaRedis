package site.hnfy258.sentinel;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.log4j.Logger;
import site.hnfy258.cluster.ClusterNode;
import site.hnfy258.coder.MyDecoder;
import site.hnfy258.coder.MyResponseEncoder;
import site.hnfy258.command.impl.Ping;
import site.hnfy258.protocal.RespArray;
import site.hnfy258.protocal.SimpleString;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

public class Sentinel {

    Logger logger = Logger.getLogger(Sentinel.class);
    private String sentinelId;
    private String host;
    private int port;

    private Map<String, Sentinel> neighborSentinels;

    private Map<String, ClusterNode> monitoredNodes;

    private Set<String> subjectiveDownNodes;
    private Set<String> objectiveDownNodes;

    private Map<String, Map<String, Boolean>> sentinelOpinons;

    private int quorum;

    private ScheduledExecutorService scheduledExecutor;

    private Map<String, Long> lastReplyTime;


    public Sentinel(String sentinelId, String host, int port, int quorum) {
        this.sentinelId = sentinelId;
        this.host = host;
        this.port = port;
        this.quorum = quorum;


        this.neighborSentinels = new ConcurrentHashMap<>();
        this.monitoredNodes = new ConcurrentHashMap<>();
        this.subjectiveDownNodes = ConcurrentHashMap.newKeySet();
        this.objectiveDownNodes = ConcurrentHashMap.newKeySet();
        this.sentinelOpinons= new ConcurrentHashMap<>();

        this.scheduledExecutor = Executors.newScheduledThreadPool(2);

    }
    public void startMonitoring() {
        //检测主观下线
        scheduledExecutor.scheduleAtFixedRate(this::checkMasterStatus,0,1,TimeUnit.SECONDS);

        //检测客观下线
        scheduledExecutor.scheduleAtFixedRate(this::exchangeSentinelInfo,0,1,TimeUnit.SECONDS);
    }

    private void exchangeSentinelInfo() {
        for(String masterId: subjectiveDownNodes){
            sentinelOpinons.putIfAbsent(masterId,new ConcurrentHashMap<>());

            sentinelOpinons.get(masterId).put(sentinelId,true);

            for(Map.Entry<String,Sentinel> entry: neighborSentinels.entrySet()){
                String neighborId = entry.getKey();
                Sentinel neighbor = entry.getValue();

                try{
                    boolean isDown = askNeighborAboutNode(neighbor,masterId);
                    sentinelOpinons.get(masterId).put(neighborId,isDown);
                }catch(Exception e){
                    logger.error("Ask neighbor about node failed",e);
                }
            }

            long downVotes = sentinelOpinons.get(masterId).values().stream().filter(isDown->isDown).count();

            if(downVotes>=quorum){
                objectiveDownNodes.add(masterId);
                logger.info("Master "+masterId+" is down");
            }

            else{
                objectiveDownNodes.remove(masterId);
            }
        }
    }

    private boolean askNeighborAboutNode(Sentinel neighbor, String masterId) {
        return false;
    }


    private void checkMasterStatus() {
        for(Map.Entry<String, ClusterNode> entry: monitoredNodes.entrySet()){
            String masterId = entry.getKey();
            ClusterNode node = entry.getValue();
            //todo 完善主观下线检测
            if(true){
                subjectiveDownNodes.add(masterId);
                node.disconnect();
            }
            else{
                //如果恢复
                subjectiveDownNodes.remove(masterId);
                if(node.isActive()){
                    node.connect();
                }
            }
        }
    }
}
