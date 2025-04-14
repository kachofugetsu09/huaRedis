package site.hnfy258.cluster;

import site.hnfy258.datatype.BytesWrapper;

import java.util.ArrayList;
import java.util.List;

public class ConsistentHashSharding implements ShardingStrategy {
    private static final int HASH_SLOTS = 16384;
    private final int[] slots = new int[HASH_SLOTS];
    private final List<String> nodes;

    public ConsistentHashSharding(List<String> nodes) {
        this.nodes = new ArrayList<>(nodes);
        distributeSlots();
    }

    private void distributeSlots() {
        int nodesCount = nodes.size();
        int slotsPerNode = HASH_SLOTS / nodesCount;
        int remainingSlots = HASH_SLOTS % nodesCount;

        int slot = 0;
        for (int nodeIndex = 0; nodeIndex < nodesCount; nodeIndex++) {
            int endSlot = slot + slotsPerNode + (nodeIndex < remainingSlots ? 1 : 0);
            while (slot < endSlot) {
                slots[slot] = nodeIndex;
                slot++;
            }
        }

        // 打印槽位分配信息
        System.out.println("Slot distribution:");
        for (int nodeIndex = 0; nodeIndex < nodesCount; nodeIndex++) {
            int startSlot = nodeIndex * slotsPerNode + Math.min(nodeIndex, remainingSlots);
            int endSlot = startSlot + slotsPerNode + (nodeIndex < remainingSlots ? 1 : 0) - 1;
            System.out.printf("Node %s: slots %d-%d%n", nodes.get(nodeIndex), startSlot, endSlot);
        }
    }

    @Override
    public String getNodeForKey(BytesWrapper key) {
        int slot = getSlot(key);
        return nodes.get(slots[slot]);
    }

    private int getSlot(BytesWrapper key) {
        int crc = CRC16.crc16(key.getBytes());
        int slot = crc % HASH_SLOTS;
        System.out.printf("Key '%s': CRC=%d, Slot=%d%n", key.toUtf8String(), crc, slot);
        return slot;
    }
}
