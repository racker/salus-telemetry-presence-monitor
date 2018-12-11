
package com.rackspace.salus.telemetry.presence_monitor.services;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.data.KeyValue;
import com.coreos.jetcd.kv.GetResponse;
import com.coreos.jetcd.watch.WatchResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.salus.common.workpart.Bits;
import com.rackspace.salus.common.workpart.WorkProcessor;
import com.rackspace.salus.telemetry.etcd.services.EnvoyResourceManagement;
import com.rackspace.salus.telemetry.etcd.types.Keys;
import com.rackspace.salus.telemetry.model.ResourceInfo;
import com.rackspace.salus.telemetry.presence_monitor.types.KafkaMessageType;
import com.rackspace.salus.telemetry.presence_monitor.types.PartitionSlice;
import com.rackspace.salus.telemetry.presence_monitor.types.PartitionWatcher;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@Data
public class PresenceMonitorProcessor implements WorkProcessor {
    private ConcurrentHashMap<String, PartitionSlice> partitionTable;
    private Client etcd;
    private ObjectMapper objectMapper;
    private EnvoyResourceManagement envoyResourceManagement;
    private ThreadPoolTaskScheduler taskScheduler;
    private MetricExporter metricExporter;
    private Boolean exporterStarted = false;

    private final MeterRegistry meterRegistry;
    private final Counter startedWork;
    private final Counter updatedWork;
    private final Counter stoppedWork;

    @Autowired
    PresenceMonitorProcessor(Client etcd, ObjectMapper objectMapper,
                             EnvoyResourceManagement envoyResourceManagement,
                             ThreadPoolTaskScheduler taskScheduler, MetricExporter metricExporter,
                             MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        partitionTable = new ConcurrentHashMap<>();
        this.objectMapper = objectMapper;
        this.etcd = etcd;
        this.envoyResourceManagement = envoyResourceManagement;
        this.taskScheduler = taskScheduler;
        this.metricExporter = metricExporter;
        this.metricExporter.setPartitionTable(partitionTable);

        startedWork = meterRegistry.counter("workProcessorChange", "state", "started");
        updatedWork = meterRegistry.counter("workProcessorChange", "state", "updated");
        stoppedWork = meterRegistry.counter("workProcessorChange", "state", "stopped");
        meterRegistry.gaugeMapSize("partitionSlices", Collections.emptyList(), partitionTable);
    }

    private String getExpectedId(KeyValue kv) {
        String[] strings = kv.getKey().toStringUtf8().split("/");
        return strings[strings.length - 1];
    }

    @Override
    public void start(String id, String content) {
        log.info("Starting work on id={}, content={}", id, content);
        startedWork.increment();

        if (!exporterStarted) {
            taskScheduler.submit(metricExporter);
            exporterStarted = true;
        }
        PartitionSlice newSlice = new PartitionSlice();
        meterRegistry.gaugeMapSize("partitionExpectedSize",
            Collections.singletonList(Tag.of("id", id)),
            newSlice.getExpectedTable());

        JsonNode workContent;
        try {
            workContent = objectMapper.readTree(content);
        } catch (IOException e) {
            log.error("Invalid content {}.  Skipping.", content);
            return;
        }
        newSlice.setRangeMax(workContent.get("end").asText());
        newSlice.setRangeMin(workContent.get("start").asText());

        // Get the expected entries
        GetResponse expectedResponse = envoyResourceManagement.getResourcesInRange(Keys.FMT_RESOURCES_EXPECTED, newSlice.getRangeMin(),
                newSlice.getRangeMax()).join();
        expectedResponse.getKvs().forEach(kv -> {
            // Create an entry for the kv
            String k = getExpectedId(kv);
            ResourceInfo resourceInfo;
            PartitionSlice.ExpectedEntry expectedEntry = new PartitionSlice.ExpectedEntry();
            try {
                resourceInfo = objectMapper.readValue(kv.getValue().getBytes(), ResourceInfo.class);
            } catch (IOException e) {
                log.warn("Failed to parse ResourceInfo", e);
                return;
            }
            expectedEntry.setResourceInfo(resourceInfo);
            expectedEntry.setActive(false);
            newSlice.getExpectedTable().put(k, expectedEntry);
        });

        // Get the active  entries
        GetResponse activeResponse = envoyResourceManagement.getResourcesInRange(Keys.FMT_RESOURCES_ACTIVE, newSlice.getRangeMin(),
                newSlice.getRangeMax()).join();
        activeResponse.getKvs().forEach(activeKv -> {
            // Update entry for the kv
            String activeKey = getExpectedId(activeKv);
            PartitionSlice.ExpectedEntry entry = newSlice.getExpectedTable().get(activeKey);
            if (entry == null) {
                log.warn("Entry is null for key {}", activeKey);
                entry = new PartitionSlice.ExpectedEntry();
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

        newSlice.setExpectedWatch(new PartitionWatcher("expected-" + id,
                taskScheduler, Keys.FMT_RESOURCES_EXPECTED,
                expectedResponse.getHeader().getRevision(),
                newSlice, expectedWatchResponseConsumer,
                envoyResourceManagement));
        newSlice.getExpectedWatch().start();

        newSlice.setActiveWatch(new PartitionWatcher("active-" + id,
                taskScheduler, Keys.FMT_RESOURCES_ACTIVE,
                activeResponse.getHeader().getRevision(),
                newSlice, activeWatchResponseConsumer,
                envoyResourceManagement));
        newSlice.getActiveWatch().start();

        partitionTable.put(id, newSlice);

    }


    // Handle watch events from the expected keys
    BiConsumer<WatchResponse, PartitionSlice> expectedWatchResponseConsumer = (watchResponse, partitionSlice) ->
        watchResponse.getEvents().forEach(event -> {
            String eventKey;
            ResourceInfo resourceInfo;
            PartitionSlice.ExpectedEntry watchEntry;
            if (Bits.isNewKeyEvent(event) || Bits.isUpdateKeyEvent(event)) {
                // add new entry
                eventKey = getExpectedId(event.getKeyValue());
                watchEntry = new PartitionSlice.ExpectedEntry();
                try {
                    resourceInfo = objectMapper.readValue(event.getKeyValue().getValue().getBytes(), ResourceInfo.class);
                } catch (IOException e) {
                    log.warn("Failed to parse ResourceInfo {}", e);
                    return;
                }
                watchEntry.setResourceInfo(resourceInfo);
                watchEntry.setActive(false);
                partitionSlice.getExpectedTable().put(eventKey, watchEntry);
            } else {
                // Delete old entry
                eventKey = getExpectedId(event.getPrevKV());

                if (partitionSlice.getExpectedTable().remove(eventKey) == null) {
                    log.warn("Failed to find ExpectedEntry to delete {}", eventKey);
                }

            }
        });


    // Handle watch events from the active keys
    BiConsumer<WatchResponse, PartitionSlice> activeWatchResponseConsumer = (watchResponse, partitionSlice) ->
        watchResponse.getEvents().forEach(event -> {
            String eventKey;
            ResourceInfo resourceInfo;
            Boolean activeValue = false;
            if (Bits.isNewKeyEvent(event) || Bits.isUpdateKeyEvent(event)) {
                eventKey = getExpectedId(event.getKeyValue());
                activeValue = true;
            } else {
                eventKey = getExpectedId(event.getPrevKV());
            }
            if (partitionSlice.getExpectedTable().containsKey(eventKey)) {
                PartitionSlice.ExpectedEntry expectedEntry = partitionSlice.getExpectedTable().get(eventKey);
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

    @Override
    public void update(String id, String content) {
        log.info("Updating work on id={}, content={}", id, content);
        updatedWork.increment();

        stop(id, content);
        start(id, content);
    }

    @Override
    public void stop(String id, String content) {
        log.info("Stopping work on id={}, content={}", id, content);
        stoppedWork.increment();

        PartitionSlice slice = partitionTable.get(id);
        if (slice != null) {
            partitionTable.remove(id);
            slice.getActiveWatch().stop();
            slice.getExpectedWatch().stop();
        }
    }
}
