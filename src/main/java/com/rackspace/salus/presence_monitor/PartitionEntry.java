package com.rackspace.salus.presence_monitor;

import com.coreos.jetcd.Watch;
import com.coreos.jetcd.common.exception.ClosedClientException;
import com.coreos.jetcd.watch.WatchResponse;
import lombok.Data;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import com.rackspace.salus.telemetry.model.NodeInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

@Data
public class PartitionEntry {
    @Data
    public static class ExistanceEntry {
        Boolean active;
        NodeInfo nodeInfo;
    }
    @Data
    @Slf4j
    public static class PartitionWatcher {
        Boolean running = false;
        final String name;
        final ThreadPoolTaskScheduler taskScheduler;
        final Watch.Watcher watcher;
        final PartitionEntry partitionEntry;
        final BiConsumer<WatchResponse, PartitionEntry> watchResponseConsumer;
        void start() {
            running = true;
            taskScheduler.submit(() -> {
                log.info("Watching {}", name);
                while (running) {
                    try {
                        final WatchResponse watchResponse = watcher.listen();
                        if (running) {
                            watchResponseConsumer.accept(watchResponse, partitionEntry);
                        }
                    } catch (ClosedClientException e) {
                        log.debug("Stopping watching of {}", name);
                        return;
                    } catch (InterruptedException e) {
                        log.debug("Interrupted while watching {}", name);
                    } catch (Exception e) {
                        log.warn("Failed while watching {}", name, e);
                        return;
                    }
                }
                log.debug("Finished watching {}", name);
            });

        }
        void stop() {
            running = false;
        }
    }
    PartitionEntry() {
        existanceTable = new ConcurrentHashMap<>();
    }
    String rangeMin;
    String rangeMax;
    PartitionWatcher existsWatch;
    PartitionWatcher activeWatch;
    ConcurrentHashMap<String, ExistanceEntry> existanceTable;
}
