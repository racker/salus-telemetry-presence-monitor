package com.rackspace.salus.telemetry.presence_monitor.services;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;


import com.coreos.jetcd.Client;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.watch.WatchResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.salus.telemetry.etcd.config.KeyHashing;
import com.rackspace.salus.telemetry.etcd.services.EnvoyResourceManagement;
import com.rackspace.salus.telemetry.model.ResourceInfo;
import com.rackspace.salus.telemetry.presence_monitor.config.PresenceMonitorProperties;
import com.rackspace.salus.telemetry.presence_monitor.types.KafkaMessageType;
import com.rackspace.salus.telemetry.presence_monitor.types.PartitionEntry;
import io.etcd.jetcd.launcher.junit.EtcdClusterResource;

import java.net.URI;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)

public class PresenceMonitorProcessorTest {
    @Configuration
    @Import({KeyHashing.class, MetricExporter.class, PresenceMonitorProperties.class})
    public static class TestConfig {

    }

    @Rule
    public final EtcdClusterResource etcd = new EtcdClusterResource("PresenceMonitorProcessorTest", 1);

    private ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    MetricExporter metricExporter;

    @MockBean
    MetricRouter metricRouter;

    private ThreadPoolTaskScheduler taskScheduler;

    private EnvoyResourceManagement envoyResourceManagement;

    private Client client;

    @Autowired
    KeyHashing hashing;

    private String expectedResourceInfoString =
            "{\"identifier\":\"os\",\"identifierValue\":\"LINUX\"," +
                    "\"labels\":{\"os\":\"LINUX\",\"arch\":\"X86_64\"},\"envoyId\":\"abcde\"," +
                    "\"tenantId\":\"123456\",\"address\":\"host:1234\"}";

    private ResourceInfo expectedResourceInfo;

    private String activeResourceInfoString = expectedResourceInfoString.replace("host:1234", "host2:3456");

    private ResourceInfo activeResourceInfo;

    private String rangeStart = "0000000000000000000000000000000000000000000000000000000000000000",
            rangeEnd = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";


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
        PresenceMonitorProperties presenceMonitorProperties = new PresenceMonitorProperties();
        presenceMonitorProperties.setExportPeriodInSeconds(1);
        metricExporter = new MetricExporter(metricRouter, presenceMonitorProperties);
    }


    @Test
    public void testProcessorStart() throws Exception {
        Semaphore routerSem = new Semaphore(0);
        doAnswer((a) -> {
            routerSem.release();
            return null;
        }).when(metricRouter).route(any(), any());

        client.getKVClient().put(
                ByteSequence.fromString("/resources/expected/1"),
                ByteSequence.fromString(expectedResourceInfoString)).join();

        client.getKVClient().put(
                ByteSequence.fromString("/resources/active/1"),
                ByteSequence.fromString(activeResourceInfoString)).join();


        PresenceMonitorProcessor p = new PresenceMonitorProcessor(client, objectMapper,
                envoyResourceManagement, taskScheduler, metricExporter);


        p.start("id1", "{" +
                "\"start\":\"" + rangeStart + "\"," +
                "\"end\":\"" + rangeEnd + "\"}");

        PartitionEntry partitionEntry = p.getPartitionTable().get("id1");
        assertEquals("range start should be all zeros", rangeStart, partitionEntry.getRangeMin());
        assertEquals("range end should be all f's", rangeEnd, partitionEntry.getRangeMax());

        PartitionEntry.ExpectedEntry expectedEntry = partitionEntry.getExpectedTable().get("1");
        assertEquals(activeResourceInfo, expectedEntry.getResourceInfo());
        assertEquals(true, expectedEntry.getActive());
        routerSem.acquire();
        verify(metricRouter).route(expectedEntry, KafkaMessageType.METRIC);

        p.stop("id1", "{" +
                "\"start\":\"" + rangeStart + "\"," +
                "\"end\":\"" + rangeEnd + "\"}");

        assertEquals(p.getPartitionTable().size(), 0);
        assertEquals(partitionEntry.getExpectedWatch().getRunning(), false);
        assertEquals(partitionEntry.getActiveWatch().getRunning(), false);
    }

    @Test
    public void testProcessorWatchConsumers() throws Exception {
        doNothing().when(metricRouter).route(any(), any());

        PresenceMonitorProcessor p = new PresenceMonitorProcessor(client, objectMapper,
                envoyResourceManagement, taskScheduler, metricExporter);


        // wrap expected watch consumer to release a semaphore when done
        Semaphore expectedSem = new Semaphore(0);
        BiConsumer<WatchResponse, PartitionEntry> originalExpectedConsumer = p.getExpectedWatchResponseConsumer();
        BiConsumer<WatchResponse, PartitionEntry> newExpectedConsumer = (wr, pe) -> {
            originalExpectedConsumer.accept(wr, pe);
            expectedSem.release();
        };
        p.setExpectedWatchResponseConsumer(newExpectedConsumer);

        // wrap active watch consumer to release a semaphore when done
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
        verify(metricRouter).route(partitionEntry.getExpectedTable().get("1"), KafkaMessageType.EVENT);

        // Now delete active entry and see it go inactive
        client.getKVClient().delete(ByteSequence.fromString("/resources/active/1"));
        activeSem.acquire();

        assertEquals("Entry should be inactive",
                partitionEntry.getExpectedTable().get("1").getActive(), false);
        assertEquals(activeResourceInfo,
                partitionEntry.getExpectedTable().get("1").getResourceInfo());

        // Now delete expected entry and see it removed from the table
        client.getKVClient().delete(ByteSequence.fromString("/resources/expected/1"));
        expectedSem.acquire();

        assertEquals("Entry should be gone",
                partitionEntry.getExpectedTable().containsKey("1"), false);
    }
}
