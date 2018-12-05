package com.rackspace.salus.presence_monitor;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.kv.GetResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.salus.telemetry.etcd.config.KeyHashing;
import com.rackspace.salus.telemetry.etcd.services.EnvoyNodeManagement;
import com.rackspace.salus.telemetry.etcd.types.Keys;
import com.rackspace.salus.telemetry.model.NodeInfo;
import lombok.extern.slf4j.Slf4j;
import com.rackspace.salus.common.workpart.WorkProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import sun.jvm.hotspot.runtime.ObjectMonitor;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.security.Key;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Slf4j
public class PresenceMonitorProcessor implements WorkProcessor {
  private ConcurrentHashMap <String, PartitionEntry> partitionTable;
  private ObjectMapper objectMapper;
  private Client etcd;
  private KeyHashing hashing;
  private EnvoyNodeManagement envoyNodeManagement;
  @Autowired
  PresenceMonitorProcessor(Client etcd, ObjectMapper objectMapper, KeyHashing hashing, EnvoyNodeManagement envoyNodeManagement) {
    partitionTable = new ConcurrentHashMap<>();
    this.objectMapper = objectMapper;
    this.etcd = etcd;
    this.hashing = hashing;
    this.envoyNodeManagement = envoyNodeManagement;

    Map<String, String> envoyLabels = new HashMap<>();
    envoyLabels.put("os", "LINUX");
    envoyLabels.put("arch", "X86_64");

    String envoyId = "abcde";
    String tenantId = "123456";
    String identifier = "os";
    String identifierValue = envoyLabels.get(identifier);
    long leaseId = 50;
    SocketAddress address;
    try {
      address = new InetSocketAddress(InetAddress.getLocalHost(), 1234);
    } catch (UnknownHostException e) {

      address = null;
    }

    String nodeKey = String.format("%s:%s:%s", tenantId, identifier, identifierValue);
    final String nodeKeyHash = hashing.hash(nodeKey);

    NodeInfo nodeInfo = new NodeInfo()
            .setEnvoyId(envoyId)
            .setIdentifier(identifier)
            .setIdentifierValue(identifierValue)
            .setLabels(envoyLabels)
            .setTenantId(tenantId);
         //   .setAddress(address);






    //envoyNodeManagement.registerNode(tenantId, envoyId, leaseId, identifier, envoyLabels, address).join();

  }
  @Override
  public void start(String id, String content)
  {
    log.info("GBJ4 Starting work on id={}, content={}", id, content);
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
    GetResponse existResponse = envoyNodeManagement.getNodesInRange(Keys.FMT_NODES_EXPECTED, newEntry.getRangeMin(),
            newEntry.getRangeMax()).join();
    existResponse.getKvs().stream().forEach(kv -> {
      String k = kv.getKey().toStringUtf8().substring(16);
      NodeInfo nodeInfo;
      PartitionEntry.ExistanceEntry existanceEntry = new PartitionEntry.ExistanceEntry();
      try {
        nodeInfo = objectMapper.readValue(kv.getValue().getBytes(), NodeInfo.class);
      } catch (IOException e) {
        log.warn("Failed to parse NodeInfo", e);
        return;
      }
      existanceEntry.setNodeInfo(nodeInfo);
      existanceEntry.setActive(false);
      newEntry.getExistanceTable().put(k,existanceEntry);
      GetResponse activeResponse = envoyNodeManagement.getNodesInRange(Keys.FMT_NODES_ACTIVE, newEntry.getRangeMin(),
              newEntry.getRangeMax()).join();
      activeResponse.getKvs().stream().forEach(activeKv -> {
        String activeKey = activeKv.getKey().toStringUtf8().substring(14);
        newEntry.getExistanceTable().get(activeKey).setActive(true);
      });

    });


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
