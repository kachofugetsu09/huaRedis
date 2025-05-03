package site.hnfy258.cluster.replication;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import site.hnfy258.cluster.ClusterClient;
import site.hnfy258.cluster.ClusterNode;
import site.hnfy258.protocal.RespArray;
import site.hnfy258.server.MyRedisService;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

public class ReplicationHandler {
    private ClusterNode masterNode;
    private List<ClusterNode> slaveNodes;
    private ScheduledExecutorService scheduler;

    public ReplicationHandler(ClusterNode masterNode){
        if (masterNode == null) {
            throw new IllegalArgumentException("Master node cannot be null");
        }
        this.masterNode = masterNode;
        this.slaveNodes = masterNode.getSlaves();
        // Handle the case where the master node doesn't have any slaves yet
        if (this.slaveNodes == null) {
            this.slaveNodes = new ArrayList<>();
        }
    }

    public List<ClusterNode> getSlaveNodes() {
        return slaveNodes;
    }

    public void handle(RespArray commandArray) {
        try {
            // Get the master service
            MyRedisService masterService = masterNode.getService();
            if (masterService == null) {
                System.err.println("Master service is null, cannot replicate command");
                return;
            }

            // Check if we have any slaves to replicate to
            if (slaveNodes == null || slaveNodes.isEmpty()) {
                // No slaves to replicate to
                return;
            }

            // Send the command to each slave
            for (ClusterNode slaveNode : slaveNodes) {
                if (slaveNode == null) continue;

                try {
                    System.out.println("Replicating command to slave: " + slaveNode.getId());
                    masterService.sendMessageToNode(slaveNode.getId(), commandArray);
                } catch (Exception e) {
                    System.err.println("Error replicating command to slave " + slaveNode.getId() + ": " + e.getMessage());
                }
            }
        } catch (Exception e) {
            System.err.println("Error in replication handler: " + e.getMessage());
            e.printStackTrace();
        }
    }

}
