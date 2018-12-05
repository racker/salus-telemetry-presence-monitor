package com.rackspace.salus.presence_monitor;

import com.coreos.jetcd.Watch;
import lombok.Data;

import java.util.concurrent.ConcurrentHashMap;

import com.rackspace.salus.telemetry.model.NodeInfo;

@Data
public class PartitionEntry {
    @Data
    public static class ExistanceEntry {
        Boolean active;
        NodeInfo nodeInfo;
    }
    PartitionEntry() {
        existanceTable = new ConcurrentHashMap<>();
    }
    String rangeMin;
    String rangeMax;
    Watch.Watcher existsWatch;
    Watch.Watcher activeWatch;
    ConcurrentHashMap<String, ExistanceEntry> existanceTable;
}
