package site.hnfy258.cluster;

import java.util.HashSet;
import java.util.Set;

public class ClusterNode {
    private String id;
    private String ip;
    private int port;
    private boolean isMaster;
    private Set<ClusterNode> slaves;

    public ClusterNode(String nodeId, String ip, int port, boolean isMaster) {
        this.id = nodeId;
        this.ip = ip;
        this.port = port;
        this.isMaster = isMaster;
    }

    public void addSlave(ClusterNode slave) {
        if (slaves == null) {
            slaves = new HashSet<>();
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
}