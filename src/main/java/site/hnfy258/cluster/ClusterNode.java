package site.hnfy258.cluster;

import site.hnfy258.server.MyRedisService;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ClusterNode {
    private final String id;
    private final String ip;
    private final int port;
    private boolean isMaster;
    private List<ClusterNode> slaves;
    private MyRedisService service;

    public ClusterNode(String nodeId, String ip, int port, boolean isMaster) {
        this.id = nodeId;
        this.ip = ip;
        this.port = port;
        this.isMaster = isMaster;
    }

    public void addSlave(ClusterNode slave) {
        if (slaves == null) {
            slaves = new ArrayList<>();
        }
        slaves.add(slave);
    }

    public String getId() {
        return id;
    }

    public int getPort() {
        return port;
    }

    public String getIp() {
        return ip;
    }

    public boolean isMaster() {
        return isMaster;
    }

    public List<ClusterNode> getSlaves() {
        return slaves;
    }

    public MyRedisService getService() {
        return service;
    }

    public void setService(MyRedisService service) {
        this.service = service;
    }

    public void setMaster(boolean b) {
        isMaster = b;
    }
}