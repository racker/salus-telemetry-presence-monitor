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

  @Mock
  ThreadPoolTaskScheduler taskScheduler;


  EnvoyResourceManagement envoyResourceManagement;

  Client client;

  @Autowired
  KeyHashing hashing;

  @Before
  public void setUp() throws Exception {
    final List<String> endpoints = etcd.cluster().getClientEndpoints().stream()
            .map(URI::toString)
            .collect(Collectors.toList());
    client = com.coreos.jetcd.Client.builder().endpoints(endpoints).build();
    envoyResourceManagement = new EnvoyResourceManagement(client, objectMapper, hashing);
  }


  @Test
  public void testProcessorStart() throws ExecutionException, InterruptedException {
    doNothing().when(metricExporter).run();
    when(taskScheduler.submit((Runnable)any())).thenReturn(null);

    PresenceMonitorProcessor p = new PresenceMonitorProcessor(client, objectMapper,
            envoyResourceManagement,taskScheduler, metricExporter);

    client.getKVClient().put(
            ByteSequence.fromString("/resources/active/r1"),
            ByteSequence.fromString(
                    "{\"identifier\":\"os\",\"identifierValue\":\"LINUX\"," +
                            "\"labels\":{\"os\":\"LINUX\",\"arch\":\"X86_64\"},\"envoyId\":\"abcde\"," +
                            "\"tenantId\":\"123456\",\"address\":\"host:1234\"}")
    ).join();

    client.getKVClient().put(
            ByteSequence.fromString("/resources/expected/r1"),
            ByteSequence.fromString(
                    "{\"identifier\":\"os\",\"identifierValue\":\"LINUX\"," +
                            "\"labels\":{\"os\":\"LINUX\",\"arch\":\"X86_64\"},\"envoyId\":\"abcde\"," +
                            "\"tenantId\":\"123456\",\"address\":\"host:1234\"}")
    ).join();
    p.start("id1", "{" +
            "\"start\":\"0000000000000000000000000000000000000000000000000000000000000000\"," +
            "\"end\":\"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff\"}");

    PartitionEntry partitionEntry = p.getPartitionTable().get("id1");
    System.out.println("hi");


  }

}
