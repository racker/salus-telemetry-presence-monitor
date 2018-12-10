package com.rackspace.salus.telemetry.presence_monitor.types;

import com.coreos.jetcd.Watch;
import com.coreos.jetcd.common.exception.ClosedClientException;
import com.coreos.jetcd.watch.WatchResponse;
import com.rackspace.salus.telemetry.etcd.services.EnvoyResourceManagement;
import lombok.Data;
import java.util.function.BiConsumer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

@Data
@Slf4j
@Component
public class PartitionWatcher {
    final String name;
    final ThreadPoolTaskScheduler taskScheduler;
    final String prefix;
    final Long revision;
    final PartitionEntry partitionEntry;
    final BiConsumer<WatchResponse, PartitionEntry> watchResponseConsumer;
    final EnvoyResourceManagement envoyResourceManagement;
    Watch.Watcher watcher;
    Boolean running = false;

    public void start() {
        watcher = envoyResourceManagement.getWatchOverRange(prefix,
                partitionEntry.getRangeMin(), partitionEntry.getRangeMax(), revision);

        taskScheduler.submit(() -> {
            running = true;
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
