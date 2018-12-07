
package com.rackspace.salus.telemetry.presence_monitor.services;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.Watch;
import com.coreos.jetcd.kv.GetResponse;
import com.coreos.jetcd.watch.WatchResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.salus.common.workpart.Bits;
import com.rackspace.salus.telemetry.presence_monitor.types.PartitionEntry;
import com.rackspace.salus.telemetry.etcd.config.KeyHashing;
import com.rackspace.salus.telemetry.etcd.services.EnvoyResourceManagement;
import com.rackspace.salus.telemetry.etcd.types.Keys;
import com.rackspace.salus.telemetry.model.ResourceInfo;
import lombok.extern.slf4j.Slf4j;
import com.rackspace.salus.common.workpart.WorkProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

@Component
@Slf4j
public class PresenceMonitorProcessor implements WorkProcessor {
  private ConcurrentHashMap <String, PartitionEntry> partitionTable;
  private ObjectMapper objectMapper;
  private Client etcd;
  private KeyHashing hashing;
  private EnvoyResourceManagement envoyResourceManagement;
  private ThreadPoolTaskScheduler taskScheduler;
  private MetricExporter metricExporter;
  @Autowired
  PresenceMonitorProcessor(Client etcd, ObjectMapper objectMapper, KeyHashing hashing,
                           EnvoyResourceManagement envoyResourceManagement, ThreadPoolTaskScheduler taskScheduler,
                           MetricExporter metricExporter) {
    partitionTable = new ConcurrentHashMap<>();
    this.objectMapper = objectMapper;
    this.etcd = etcd;
    this.hashing = hashing;
    this.envoyResourceManagement = envoyResourceManagement;
    this.taskScheduler = taskScheduler;
    this.metricExporter = metricExporter;
    this.metricExporter.setPartitionTable(partitionTable);
    taskScheduler.submit(metricExporter);

    if (false) {
    Map<String, String> envoyLabels = new HashMap<>();
    envoyLabels.put("os", "LINUX");
    envoyLabels.put("arch", "X86_64");

    String envoyId = "abcde";
    String tenantId = "123456";
    String identifier = "os";
    String identifierValue = envoyLabels.get(identifier);
    long leaseId = 50;
    InetSocketAddress address;
    try {
      address = new InetSocketAddress(InetAddress.getLocalHost(), 1234);
    } catch (UnknownHostException e) {

      address = null;
    }

    String resourceKey = String.format("%s:%s:%s", tenantId, identifier, identifierValue);
    final String resourceKeyHash = hashing.hash(resourceKey);

    ResourceInfo resourceInfo = new ResourceInfo()
            .setEnvoyId(envoyId)
            .setIdentifier(identifier)
            .setIdentifierValue(identifierValue)
            .setLabels(envoyLabels)
            .setTenantId(tenantId)
            .setAddress(address);







    envoyResourceManagement.registerResource(tenantId, envoyId, leaseId, identifier, envoyLabels, address).join();
}
  }
  @Override
  public void start(String id, String content)
  {
    log.info("GBJ6 Starting work on id={}, content={}", id, content);
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
    GetResponse expectedResponse = envoyResourceManagement.getResourcesInRange(Keys.FMT_RESOURCES_EXPECTED, newEntry.getRangeMin(),
            newEntry.getRangeMax()).join();
    expectedResponse.getKvs().stream().forEach(kv -> {
      String k = kv.getKey().toStringUtf8().substring(16);
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
    GetResponse activeResponse = envoyResourceManagement.getResourcesInRange(Keys.FMT_RESOURCES_ACTIVE, newEntry.getRangeMin(),
            newEntry.getRangeMax()).join();
    activeResponse.getKvs().stream().forEach(activeKv -> {
      String activeKey = activeKv.getKey().toStringUtf8().substring(14);
      PartitionEntry.ExpectedEntry entry = newEntry.getExpectedTable().get(activeKey);
      if (entry == null) {
        log.warn("Entry is null for key {}", activeKey);
        return;
      } else {
        entry.setActive(true);
      }
    });

    Watch.Watcher expectedWatch = envoyResourceManagement.getWatchOverRange(Keys.FMT_RESOURCES_EXPECTED,
              newEntry.getRangeMin(), newEntry.getRangeMax(), expectedResponse.getHeader().getRevision());
    newEntry.setExpectedWatch(new PartitionEntry.PartitionWatcher("expected-" + newEntry.getRangeMin(),
               taskScheduler, expectedWatch, newEntry, expectedWatchResponseConsumer));
    newEntry.getExpectedWatch().start();
    Watch.Watcher activeWatch = envoyResourceManagement.getWatchOverRange(Keys.FMT_RESOURCES_ACTIVE,
            newEntry.getRangeMin(), newEntry.getRangeMax(), activeResponse.getHeader().getRevision());
    newEntry.setActiveWatch(new PartitionEntry.PartitionWatcher("active-" + newEntry.getRangeMin(),
            taskScheduler, activeWatch, newEntry, activeWatchResponseConsumer));
    newEntry.getActiveWatch().start();
    partitionTable.put(id, newEntry);

  }


  BiConsumer<WatchResponse, PartitionEntry> expectedWatchResponseConsumer = (watchResponse, partitionEntry) -> {
    watchResponse.getEvents().stream().forEach(event -> {
      String eventKey;
      ResourceInfo resourceInfo;
      PartitionEntry.ExpectedEntry watchEntry;
      if (Bits.isNewKeyEvent(event) || Bits.isUpdateKeyEvent(event)) {
        eventKey = event.getKeyValue().getKey().toStringUtf8().substring(16);
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
        eventKey = event.getPrevKV().getKey().toStringUtf8().substring(16);
        if (partitionEntry.getExpectedTable().containsKey(eventKey)) {
          partitionEntry.getExpectedTable().remove(eventKey);
        } else {
          log.warn("Failed to find ExpectedEntry to delete {}", eventKey);
          return;
        }

      }
    });
  };

  BiConsumer<WatchResponse, PartitionEntry> activeWatchResponseConsumer = (watchResponse, partitionEntry) -> {
    watchResponse.getEvents().stream().forEach(event -> {
      String eventKey;
      Boolean activeValue = false;
      if (Bits.isNewKeyEvent(event) || Bits.isUpdateKeyEvent(event)) {
        eventKey = event.getKeyValue().getKey().toStringUtf8().substring(14);
        activeValue = true;
      } else {
        eventKey = event.getPrevKV().getKey().toStringUtf8().substring(14);
      }
      if (partitionEntry.getExpectedTable().containsKey(eventKey)) {
          partitionEntry.getExpectedTable().get(eventKey).setActive(activeValue);
      } else {
          log.warn("Failed to find ExpectedEntry to update {}", eventKey);
      }

    });
  };

  @Override
  public void update(String id, String content) {
    log.info("GBJ Updating work on id={}, content={}", id, content);
    stop(id, content);
    start(id, content);
  }

  @Override
  public void stop(String id, String content) {
    log.info("GBJ Stopping work on id={}, content={}", id, content);
    PartitionEntry entry = partitionTable.get(id);
    if (entry != null) {
      partitionTable.remove(id);
      entry.getActiveWatch().stop();
      entry.getExpectedWatch().stop();
    }
  }
}
