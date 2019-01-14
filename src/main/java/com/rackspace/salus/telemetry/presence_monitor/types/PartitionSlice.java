package com.rackspace.salus.telemetry.presence_monitor.types;

import lombok.Data;
import java.util.concurrent.ConcurrentHashMap;
import com.rackspace.salus.telemetry.model.ResourceInfo;

@Data
public class PartitionSlice {
    @Data
    public static class ExpectedEntry {
        Boolean active;
        ResourceInfo resourceInfo;
    }
    public PartitionSlice() {
        expectedTable = new ConcurrentHashMap<>();
    }
    String rangeMin;
    String rangeMax;
    // not a partition watcher any more/ kafka
    PartitionWatcher expectedWatch;
    PartitionWatcher activeWatch;
    ConcurrentHashMap<String, ExpectedEntry> expectedTable;
}
