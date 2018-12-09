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
import com.coreos.jetcd.watch.WatchResponse;
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
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
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

  @Mock
  MetricRouter metricRouter;

  ThreadPoolTaskScheduler taskScheduler;


  EnvoyResourceManagement envoyResourceManagement;

  Client client;

  @Autowired
  KeyHashing hashing;

  String expectedResourceInfoString =
          "{\"identifier\":\"os\",\"identifierValue\":\"LINUX\"," +
          "\"labels\":{\"os\":\"LINUX\",\"arch\":\"X86_64\"},\"envoyId\":\"abcde\"," +
          "\"tenantId\":\"123456\",\"address\":\"host:1234\"}";

  ResourceInfo expectedResourceInfo;
          
  String activeResourceInfoString = expectedResourceInfoString.replace("host:1234", "host2:3456");

  ResourceInfo activeResourceInfo;
          
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

    envoyResourceManagement = new EnvoyResourceManagement(client, objectMapper, hashing);
    expectedResourceInfo = objectMapper.readValue(expectedResourceInfoString, ResourceInfo.class);
    activeResourceInfo = objectMapper.readValue(activeResourceInfoString, ResourceInfo.class);
  }


  @Test
  public void testProcessorStart() throws Exception {
    doNothing().when(metricExporter).run();

    client.getKVClient().put(
            ByteSequence.fromString("/resources/expected/1"),
            ByteSequence.fromString(expectedResourceInfoString)).join();

    client.getKVClient().put(
            ByteSequence.fromString("/resources/active/1"),
            ByteSequence.fromString(activeResourceInfoString)).join();


    PresenceMonitorProcessor p = new PresenceMonitorProcessor(client, objectMapper,
            envoyResourceManagement,taskScheduler, metricExporter);


    p.start("id1", "{" +
            "\"start\":\"" + rangeStart + "\"," +
            "\"end\":\"" + rangeEnd + "\"}");

    PartitionEntry partitionEntry = p.getPartitionTable().get("id1");
    assertEquals("range start should be all zeros", rangeStart, partitionEntry.getRangeMin());
    assertEquals("range end should be all f's", rangeEnd, partitionEntry.getRangeMax());

    PartitionEntry.ExpectedEntry expectedEntry = partitionEntry.getExpectedTable().get("1");
    assertEquals(activeResourceInfo, expectedEntry.getResourceInfo());
    assertEquals(true, expectedEntry.getActive());

  }

  @Test
  public void testProcessorWatchConsumers() throws Exception {

    doNothing().when(metricExporter).run();
    when(metricExporter.getMetricRouter()).thenReturn(metricRouter);
    doNothing().when(metricRouter).route(any(), any());

    PresenceMonitorProcessor p = new PresenceMonitorProcessor(client, objectMapper,
            envoyResourceManagement,taskScheduler, metricExporter);



    // wrap expected watch consumer to release a semaphor when done
    Semaphore expectedSem = new Semaphore(0);
    BiConsumer<WatchResponse, PartitionEntry> originalExpectedConsumer = p.getExpectedWatchResponseConsumer();
    BiConsumer<WatchResponse, PartitionEntry> newExpectedConsumer = (wr, pe) -> {
          originalExpectedConsumer.accept(wr, pe);
          expectedSem.release();
    };
    p.setExpectedWatchResponseConsumer(newExpectedConsumer);

    // wrap active watch consumer to release a semaphor when done
    Semaphore activeSem = new Semaphore(0);
    BiConsumer<WatchResponse, PartitionEntry> originalActiveConsumer = p.getActiveWatchResponseConsumer();
    BiConsumer<WatchResponse, PartitionEntry> newActiveConsumer = (wr, pe) -> {
      originalActiveConsumer.accept(wr, pe);
      activeSem.release();
    };
    p.setActiveWatchResponseConsumer(newActiveConsumer);


    p.start("id1", "{" +
            "\"start\":\"" + rangeStart + "\"," +
            "\"end\":\"" + rangeEnd + "\"}");

    PartitionEntry partitionEntry = p.getPartitionTable().get("id1");
    assertEquals("No resources exist yet so expected table should be empty",
            partitionEntry.getExpectedTable().size(), 0);
    // Now generate an expected watch and wait for the sem
    client.getKVClient().put(
            ByteSequence.fromString("/resources/expected/1"),
            ByteSequence.fromString(expectedResourceInfoString));
    expectedSem.acquire();

    assertEquals("The watch should have added one entry",
            partitionEntry.getExpectedTable().size(), 1);
    assertEquals("Entry should be inactive",
            partitionEntry.getExpectedTable().get("1").getActive(), false);
    assertEquals(expectedResourceInfo,
        partitionEntry.getExpectedTable().get("1").getResourceInfo());




    // Now generate an active watch and wait for the sem
    client.getKVClient().put(
            ByteSequence.fromString("/resources/active/1"),
            ByteSequence.fromString(activeResourceInfoString));
    activeSem.acquire();

    assertEquals("Entry should be active",
            partitionEntry.getExpectedTable().get("1").getActive(), true);
    assertEquals(activeResourceInfo, 
        partitionEntry.getExpectedTable().get("1").getResourceInfo());
  }

}
