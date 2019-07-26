package com.rackspace.salus.telemetry.presence_monitor.types;

import com.rackspace.salus.telemetry.etcd.services.EnvoyResourceManagement;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.watch.WatchResponse;
import java.util.function.BiConsumer;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
public class PartitionWatcher {
    final String name;
    final String prefix;
    final Long revision;
    final PartitionSlice partitionSlice;
    final BiConsumer<WatchResponse, PartitionSlice> watchResponseConsumer;
    final EnvoyResourceManagement envoyResourceManagement;
    Watch.Watcher watcher;
    Boolean running = false;

    public void start() {
        watcher = envoyResourceManagement.createWatchOverRange(prefix,
                partitionSlice.getRangeMin(), partitionSlice.getRangeMax(), revision,
            watchResponse -> watchResponseConsumer.accept(watchResponse, partitionSlice)
        );
    }

    public void stop() {
        if (running) {
            running = false;
            watcher.close();
        }
    }
}
