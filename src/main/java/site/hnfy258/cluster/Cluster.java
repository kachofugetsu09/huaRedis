package site.hnfy258.cluster;

import site.hnfy258.datatype.BytesWrapper;
import site.hnfy258.server.MyRedisService;

public interface Cluster {
    
    public void start();

    public void stop();

    MyRedisService getNode(String nodeId);

    void initializeSharding();

    String getNodeForKey(BytesWrapper key);
}
