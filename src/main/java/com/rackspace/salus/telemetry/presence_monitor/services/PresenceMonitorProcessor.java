
package com.rackspace.salus.telemetry.presence_monitor.services;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.Watch;
import com.coreos.jetcd.data.KeyValue;
import com.coreos.jetcd.kv.GetResponse;
import com.coreos.jetcd.watch.WatchResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.salus.common.workpart.Bits;
import com.rackspace.salus.telemetry.presence_monitor.types.KafkaMessageType;
import com.rackspace.salus.telemetry.presence_monitor.types.PartitionEntry;
import com.rackspace.salus.telemetry.etcd.services.EnvoyResourceManagement;
import com.rackspace.salus.telemetry.etcd.types.Keys;
import com.rackspace.salus.telemetry.model.ResourceInfo;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import com.rackspace.salus.common.workpart.WorkProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

@Component
@Slf4j
@Data
public class PresenceMonitorProcessor implements WorkProcessor {
    private ConcurrentHashMap<String, PartitionEntry> partitionTable;
    private Client etcd;
    private ObjectMapper objectMapper;
    private EnvoyResourceManagement envoyResourceManagement;
    private ThreadPoolTaskScheduler taskScheduler;
    private MetricExporter metricExporter;
    private Boolean exporterStarted = false;

    @Autowired
    PresenceMonitorProcessor(Client etcd, ObjectMapper objectMapper,
                             EnvoyResourceManagement envoyResourceManagement,
                             ThreadPoolTaskScheduler taskScheduler, MetricExporter metricExporter) {
        partitionTable = new ConcurrentHashMap<>();
        this.objectMapper = objectMapper;
        this.etcd = etcd;
        this.envoyResourceManagement = envoyResourceManagement;
        this.taskScheduler = taskScheduler;
        this.metricExporter = metricExporter;
        this.metricExporter.setPartitionTable(partitionTable);
    }

    private String getExpectedId(KeyValue kv) {
        String[] strings = kv.getKey().toStringUtf8().split("/");
        return strings[strings.length - 1];
    }

    @Override
    public void start(String id, String content) {
        log.info("Starting work on id={}, content={}", id, content);
        if (!exporterStarted) {
            taskScheduler.submit(metricExporter);
            exporterStarted = true;
        }
        PartitionEntry newEntry = new PartitionEntry();
        JsonNode workContent;
        try {
            workContent = objectMapper.readTree(content);
        } catch (IOException e) {
            log.error("Invalid content {}.  Skipping.", content);
            return;
        }
        newEntry.setRangeMax(workContent.get("end").asText());
        newEntry.setRangeMin(workContent.get("start").asText());

        // Get the expected entries
        GetResponse expectedResponse = envoyResourceManagement.getResourcesInRange(Keys.FMT_RESOURCES_EXPECTED, newEntry.getRangeMin(),
                newEntry.getRangeMax()).join();
        expectedResponse.getKvs().stream().forEach(kv -> {
            // Create an entry for the kv
            String k = getExpectedId(kv);
            ResourceInfo resourceInfo;
            PartitionEntry.ExpectedEntry expectedEntry = new PartitionEntry.ExpectedEntry();
            try {
                resourceInfo = objectMapper.readValue(kv.getValue().getBytes(), ResourceInfo.class);
            } catch (IOException e) {
                log.warn("Failed to parse ResourceInfo", e);
                return;
            }
            expectedEntry.setResourceInfo(resourceInfo);
            expectedEntry.setActive(false);
            newEntry.getExpectedTable().put(k, expectedEntry);
        });

        // Get the active  entries
        GetResponse activeResponse = envoyResourceManagement.getResourcesInRange(Keys.FMT_RESOURCES_ACTIVE, newEntry.getRangeMin(),
                newEntry.getRangeMax()).join();
        activeResponse.getKvs().stream().forEach(activeKv -> {
            // Update entry for the kv
            String activeKey = getExpectedId(activeKv);
            PartitionEntry.ExpectedEntry entry = newEntry.getExpectedTable().get(activeKey);
            if (entry == null) {
                log.warn("Entry is null for key {}", activeKey);
                entry = new PartitionEntry.ExpectedEntry();
            }
            entry.setActive(true);
            ResourceInfo resourceInfo;
            try {
                resourceInfo = objectMapper.readValue(activeKv.getValue().getBytes(), ResourceInfo.class);
            } catch (IOException e) {
                log.warn("Failed to parse ResourceInfo", e);
                return;
            }
            entry.setResourceInfo(resourceInfo);
        });

        newEntry.setExpectedWatch(new PartitionEntry.PartitionWatcher("expected-" + id,
                taskScheduler, Keys.FMT_RESOURCES_EXPECTED,
                expectedResponse.getHeader().getRevision(),
                newEntry, expectedWatchResponseConsumer,
                envoyResourceManagement));
        newEntry.getExpectedWatch().start();

        newEntry.setActiveWatch(new PartitionEntry.PartitionWatcher("active-" + id,
                taskScheduler, Keys.FMT_RESOURCES_ACTIVE,
                activeResponse.getHeader().getRevision(),
                newEntry, activeWatchResponseConsumer,
                envoyResourceManagement));
        newEntry.getActiveWatch().start();

        partitionTable.put(id, newEntry);

    }


    // Handle watch events from the expected keys
    BiConsumer<WatchResponse, PartitionEntry> expectedWatchResponseConsumer = (watchResponse, partitionEntry) -> {
        watchResponse.getEvents().stream().forEach(event -> {
            String eventKey;
            ResourceInfo resourceInfo;
            PartitionEntry.ExpectedEntry watchEntry;
            if (Bits.isNewKeyEvent(event) || Bits.isUpdateKeyEvent(event)) {
                // add new entry
                eventKey = getExpectedId(event.getKeyValue());
                watchEntry = new PartitionEntry.ExpectedEntry();
                try {
                    resourceInfo = objectMapper.readValue(event.getKeyValue().getValue().getBytes(), ResourceInfo.class);
                } catch (IOException e) {
                    log.warn("Failed to parse ResourceInfo {}", e);
                    return;
                }
                watchEntry.setResourceInfo(resourceInfo);
                watchEntry.setActive(false);
                partitionEntry.getExpectedTable().put(eventKey, watchEntry);
            } else {
                // Delete old entry
                eventKey = getExpectedId(event.getPrevKV());

                if (partitionEntry.getExpectedTable().containsKey(eventKey)) {
                    partitionEntry.getExpectedTable().remove(eventKey);
                } else {
                    log.warn("Failed to find ExpectedEntry to delete {}", eventKey);
                    return;
                }

            }
        });
    };

    // Handle watch events from the active keys
    BiConsumer<WatchResponse, PartitionEntry> activeWatchResponseConsumer = (watchResponse, partitionEntry) -> {
        watchResponse.getEvents().stream().forEach(event -> {
            String eventKey;
            ResourceInfo resourceInfo;
            Boolean activeValue = false;
            if (Bits.isNewKeyEvent(event) || Bits.isUpdateKeyEvent(event)) {
                eventKey = getExpectedId(event.getKeyValue());
                activeValue = true;
            } else {
                eventKey = getExpectedId(event.getPrevKV());
            }
            if (partitionEntry.getExpectedTable().containsKey(eventKey)) {
                PartitionEntry.ExpectedEntry expectedEntry = partitionEntry.getExpectedTable().get(eventKey);
                if (expectedEntry.getActive() != activeValue) {
                    expectedEntry.setActive(activeValue);
                    metricExporter.getMetricRouter().route(expectedEntry, KafkaMessageType.EVENT);
                }
                // Update resource info if we have it
                if (activeValue) {
                    try {
                        resourceInfo = objectMapper.readValue(event.getKeyValue().getValue().getBytes(), ResourceInfo.class);
                    } catch (IOException e) {
                        log.warn("Failed to parse ResourceInfo {}", e);
                        return;
                    }
                    expectedEntry.setResourceInfo(resourceInfo);
                }
            } else {
                log.warn("Failed to find ExpectedEntry to update {}", eventKey);
            }
        });
    };

    @Override
    public void update(String id, String content) {
        log.info("Updating work on id={}, content={}", id, content);
        stop(id, content);
        start(id, content);
    }

    @Override
    public void stop(String id, String content) {
        log.info("Stopping work on id={}, content={}", id, content);
        PartitionEntry entry = partitionTable.get(id);
        if (entry != null) {
            partitionTable.remove(id);
            entry.getActiveWatch().stop();
            entry.getExpectedWatch().stop();
        }
    }
}
