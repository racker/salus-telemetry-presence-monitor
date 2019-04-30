/*
 *    Copyright 2019 Rackspace US, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 *
 */


package com.rackspace.salus.telemetry.presence_monitor.services;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.data.KeyValue;
import com.coreos.jetcd.kv.GetResponse;
import com.coreos.jetcd.watch.WatchResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.salus.common.util.KeyHashing;
import com.rackspace.salus.common.workpart.Bits;
import com.rackspace.salus.common.workpart.WorkProcessor;
import com.rackspace.salus.resource_management.web.client.ResourceApi;
import com.rackspace.salus.telemetry.etcd.services.EnvoyResourceManagement;
import com.rackspace.salus.telemetry.etcd.types.Keys;
import com.rackspace.salus.telemetry.model.Resource;
import com.rackspace.salus.telemetry.model.ResourceInfo;
import com.rackspace.salus.telemetry.presence_monitor.config.PresenceMonitorProperties;
import com.rackspace.salus.telemetry.messaging.KafkaMessageType;
import com.rackspace.salus.telemetry.presence_monitor.types.PartitionSlice;
import com.rackspace.salus.telemetry.presence_monitor.types.PartitionWatcher;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpMethod;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

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
    private static KeyHashing hashing = new KeyHashing();
    private final ResourceApi resourceApi;
    private final ResourceListener resourceListener;

    @Autowired
    PresenceMonitorProcessor(Client etcd, ObjectMapper objectMapper,
                             EnvoyResourceManagement envoyResourceManagement,
                             ThreadPoolTaskScheduler taskScheduler, MetricExporter metricExporter,
                             MeterRegistry meterRegistry, ResourceListener resourceListener,
                             ConcurrentHashMap<String, PartitionSlice> partitionTable,
                             ResourceApi resourceApi) {
        this.meterRegistry = meterRegistry;
        this.resourceListener = resourceListener;
        this.partitionTable = partitionTable;
        this.objectMapper = objectMapper;
        this.etcd = etcd;
        this.envoyResourceManagement = envoyResourceManagement;
        this.taskScheduler = taskScheduler;
        this.metricExporter = metricExporter;
        this.resourceApi = resourceApi;
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

    static String genExpectedId(String tenantId, String resourceId) {
        String resourceKey = String.format("%s:%s",
                tenantId, resourceId);
        return hashing.hash(resourceKey);
    }

    static boolean sliceContains(PartitionSlice slice, String expectedId) {
        return ((expectedId.compareTo(slice.getRangeMin()) >= 0) &&
                (expectedId.compareTo(slice.getRangeMax()) <= 0));
    }

    private List<Resource> getResources() {
        // Stop the resourceListener while reading from the resource manager
        synchronized (resourceListener) {
            return resourceApi.getExpectedEnvoys();
        }
    }

    static ResourceInfo convert(Resource resource) {
        if (resource == null) {
            return null;
        }
        ResourceInfo ri = new ResourceInfo();
        ri.setResourceId(resource.getResourceId());
        ri.setLabels(resource.getLabels());
        ri.setTenantId(resource.getTenantId());
        return ri;
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
        List<Resource> resources = getResources();

        log.debug("Found {} expected envoys", resources.size());
        resources.forEach(resource -> {
            // Create an entry for the resource
            ResourceInfo resourceInfo = convert(resource);
            String expectedId = genExpectedId(resourceInfo.getTenantId(), resourceInfo.getResourceId());
            if (sliceContains(newSlice, expectedId)) {
                PartitionSlice.ExpectedEntry expectedEntry = new PartitionSlice.ExpectedEntry();
                expectedEntry.setResourceInfo(resourceInfo);
                expectedEntry.setActive(false);
                newSlice.getExpectedTable().put(expectedId, expectedEntry);
                log.trace("record {} used to update slice", expectedId);
            } else {
                log.trace("record {} ignored", expectedId);
            }
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

        newSlice.setActiveWatch(new PartitionWatcher("active-" + id,
                taskScheduler, Keys.FMT_RESOURCES_ACTIVE,
                activeResponse.getHeader().getRevision(),
                newSlice, activeWatchResponseConsumer,
                envoyResourceManagement));
        newSlice.getActiveWatch().start();

        partitionTable.put(id, newSlice);

    }


    // Handle watch events from the active keys
    BiConsumer<WatchResponse, PartitionSlice> activeWatchResponseConsumer = (watchResponse, partitionSlice) ->
            watchResponse.getEvents().forEach(event -> {
                String eventKey;
                ResourceInfo resourceInfo;
                Boolean activeValue = false;
                PartitionSlice.ExpectedEntry expectedEntry;
                if (Bits.isNewKeyEvent(event) || Bits.isUpdateKeyEvent(event)) {
                    eventKey = getExpectedId(event.getKeyValue());
                    activeValue = true;
                } else {
                    eventKey = getExpectedId(event.getPrevKV());
                }
                if (partitionSlice.getExpectedTable().containsKey(eventKey)) {
                    expectedEntry = partitionSlice.getExpectedTable().get(eventKey);
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
                    expectedEntry = new PartitionSlice.ExpectedEntry();
                    try {
                        resourceInfo = objectMapper.readValue(event.getKeyValue().getValue().getBytes(), ResourceInfo.class);
                    } catch (IOException e) {
                        log.warn("Failed to parse ResourceInfo {}", e);
                        return;
                    }
                    expectedEntry.setResourceInfo(resourceInfo);
                    expectedEntry.setActive(activeValue);
                    partitionSlice.getExpectedTable().put(eventKey, expectedEntry);
                    metricExporter.getMetricRouter().route(expectedEntry, KafkaMessageType.EVENT);
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
        }
    }

    public ConcurrentHashMap<String, PartitionSlice> getPartitionTable() {
        return partitionTable;
    }
}
