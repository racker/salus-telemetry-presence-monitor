package com.rackspace.salus.telemetry.presence_monitor.services;

import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;


import com.coreos.jetcd.Client;
import com.coreos.jetcd.data.ByteSequence;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.salus.telemetry.etcd.EtcdUtils;
import com.rackspace.salus.telemetry.etcd.config.KeyHashing;
import com.rackspace.salus.telemetry.etcd.services.EnvoyResourceManagement;
import com.rackspace.salus.telemetry.etcd.types.KeyRange;
import com.rackspace.salus.telemetry.etcd.types.Keys;
import com.rackspace.salus.telemetry.etcd.types.WorkAllocationRealm;
import com.rackspace.salus.telemetry.model.ResourceInfo;
import com.rackspace.salus.telemetry.presence_monitor.types.PartitionEntry;
import io.etcd.jetcd.launcher.junit.EtcdClusterResource;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)

public class PresenceMonitorProcessorTest {
  @Configuration
  @Import({KeyHashing.class})
  public static class TestConfig {

  }

  @Rule
  public final EtcdClusterResource etcd = new EtcdClusterResource("PresenceMonitorProcessorTest", 1);

  ObjectMapper objectMapper = new ObjectMapper();

  @Mock
  MetricExporter metricExporter;

  
  ThreadPoolTaskScheduler taskScheduler;


  EnvoyResourceManagement envoyResourceManagement;

  Client client;

  @Autowired
  KeyHashing hashing;

  String resourceInfoString =
          "{\"identifier\":\"os\",\"identifierValue\":\"LINUX\"," +
          "\"labels\":{\"os\":\"LINUX\",\"arch\":\"X86_64\"},\"envoyId\":\"abcde\"," +
          "\"tenantId\":\"123456\",\"address\":\"host:1234\"}";

  String rangeStart = "0000000000000000000000000000000000000000000000000000000000000000",
         rangeEnd   = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
  
  @Before
  public void setUp() throws Exception {
    taskScheduler = new ThreadPoolTaskScheduler();
    taskScheduler.setPoolSize(Integer.MAX_VALUE);
    taskScheduler.setThreadNamePrefix("tasks-");
    taskScheduler.initialize();

    final List<String> endpoints = etcd.cluster().getClientEndpoints().stream()
            .map(URI::toString)
            .collect(Collectors.toList());
    client = com.coreos.jetcd.Client.builder().endpoints(endpoints).build();
    client.getKVClient().put(
            ByteSequence.fromString("/resources/active/1"),
            ByteSequence.fromString(resourceInfoString)).join();

    client.getKVClient().put(
            ByteSequence.fromString("/resources/expected/1"),
            ByteSequence.fromString(resourceInfoString)).join();

    envoyResourceManagement = new EnvoyResourceManagement(client, objectMapper, hashing);
  }


  @Test
  public void testProcessorStart() throws Exception {
    doNothing().when(metricExporter).run();

    PresenceMonitorProcessor p = new PresenceMonitorProcessor(client, objectMapper,
            envoyResourceManagement,taskScheduler, metricExporter);

           
    p.start("id1", "{" +
            "\"start\":\"" + rangeStart + "\"," +
            "\"end\":\"" + rangeEnd + "\"}");

    PartitionEntry partitionEntry = p.getPartitionTable().get("id1");
    assertEquals("range start should be all zeros", rangeStart, partitionEntry.getRangeMin());
    assertEquals("range end should be all f's", rangeEnd, partitionEntry.getRangeMax());


    ResourceInfo resourceInfo = objectMapper.readValue(resourceInfoString, ResourceInfo.class);

    PartitionEntry.ExpectedEntry expectedEntry = partitionEntry.getExpectedTable().get("1");
    assertEquals(resourceInfo, expectedEntry.getResourceInfo());
    assertEquals(true, expectedEntry.getActive());

  }

}
