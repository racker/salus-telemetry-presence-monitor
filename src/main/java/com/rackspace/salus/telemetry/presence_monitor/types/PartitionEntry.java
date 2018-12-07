package com.rackspace.salus.telemetry.presence_monitor.types;

import com.coreos.jetcd.Watch;
import com.coreos.jetcd.common.exception.ClosedClientException;
import com.coreos.jetcd.watch.WatchResponse;
import lombok.Data;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

import com.rackspace.salus.telemetry.model.ResourceInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

@Data
public class PartitionEntry {
    @Data
    public static class ExpectedEntry {
        Boolean active;
        ResourceInfo resourceInfo;
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
        public void start() {
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
        public void stop() {
            if (running) {
                running = false;
                watcher.close();
            }
        }
    }
    public PartitionEntry() {
        expectedTable = new ConcurrentHashMap<>();
    }
    String rangeMin;
    String rangeMax;
    PartitionWatcher expectedWatch;
    PartitionWatcher activeWatch;
    ConcurrentHashMap<String, ExpectedEntry> expectedTable;
}