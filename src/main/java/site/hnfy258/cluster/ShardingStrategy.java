package site.hnfy258.cluster;

import site.hnfy258.datatype.BytesWrapper;

public interface ShardingStrategy {
    String getNodeForKey(BytesWrapper key);
}
