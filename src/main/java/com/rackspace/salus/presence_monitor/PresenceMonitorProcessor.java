package com.rackspace.salus.presence_monitor;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.kv.GetResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.salus.telemetry.etcd.config.KeyHashing;
import lombok.extern.slf4j.Slf4j;
import com.rackspace.salus.common.workpart.WorkProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import sun.jvm.hotspot.runtime.ObjectMonitor;

import java.io.IOException;
import java.security.Key;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Slf4j
public class PresenceMonitorProcessor implements WorkProcessor {
  private ConcurrentHashMap <String, PartitionEntry> partitionTable;
  private ObjectMapper objectMapper;
  private Client etcd;
  private KeyHashing hashing;
  @Autowired
  PresenceMonitorProcessor(Client etcd, ObjectMapper objectMapper, KeyHashing hashing) {
    partitionTable = new ConcurrentHashMap<>();
    this.objectMapper = objectMapper;
    this.etcd = etcd;
    this.hashing = hashing;
  }
  @Override
  public void start(String id, String content)
  {
    log.info("GBJ3 Starting work on id={}, content={}", id, content);
    PartitionEntry newEntry = new PartitionEntry();
    JsonNode workContent;
    try {
      workContent = objectMapper.readTree(content);
    } catch (IOException e) {
      log.error("Invalid content {}.  Skipping.", content);
      return;
    }
    newEntry.setRangeMax(workContent.get("rangeMax").asText());
    newEntry.setRangeMin(workContent.get("rangeMin").asText());
    //GetResponse existResponse = getNodesInRange()


  }

  @Override
  public void update(String id, String content) {
    log.info("GBJ Updating work on id={}, content={}", id, content);
  }

  @Override
  public void stop(String id, String content) {
    log.info("GBJ Stopping work on id={}, content={}", id, content);
  }
}
