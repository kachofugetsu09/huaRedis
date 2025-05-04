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

        // 初始化从节点列表（如果还没有）
        if (isMaster && slaves == null) {
            slaves = new ArrayList<>();
        }
    }

    /**
     * 从从节点列表中移除指定的从节点
     *
     * @param slave 要移除的从节点
     * @return 如果成功移除返回true，否则返回false
     */
    public boolean removeSlave(ClusterNode slave) {
        if (slaves == null || slave == null) {
            return false;
        }

        // 使用节点ID进行比较，因为可能传入的是不同的对象实例
        for (int i = 0; i < slaves.size(); i++) {
            ClusterNode currentSlave = slaves.get(i);
            if (currentSlave != null && currentSlave.getId().equals(slave.getId())) {
                slaves.remove(i);
                return true;
            }
        }

        return false;
    }
}