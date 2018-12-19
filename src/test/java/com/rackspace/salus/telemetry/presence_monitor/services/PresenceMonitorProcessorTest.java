package com.rackspace.salus.telemetry.presence_monitor.services;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.watch.WatchResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.salus.telemetry.etcd.config.KeyHashing;
import com.rackspace.salus.telemetry.etcd.services.EnvoyResourceManagement;
import com.rackspace.salus.telemetry.model.ResourceInfo;
import com.rackspace.salus.telemetry.presence_monitor.config.PresenceMonitorProperties;
import com.rackspace.salus.telemetry.presence_monitor.types.KafkaMessageType;
import com.rackspace.salus.telemetry.presence_monitor.types.PartitionSlice;
import io.etcd.jetcd.launcher.junit.EtcdClusterResource;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.net.URI;
import java.time.Duration;
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

    @MockBean
    SimpleMeterRegistry simpleMeterRegistry;

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
        presenceMonitorProperties.setExportPeriod(Duration.ofSeconds(1));
        metricExporter = new MetricExporter(metricRouter, presenceMonitorProperties, new SimpleMeterRegistry());
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
                envoyResourceManagement, taskScheduler, metricExporter,
                new SimpleMeterRegistry());


        p.start("id1", "{" +
                "\"start\":\"" + rangeStart + "\"," +
                "\"end\":\"" + rangeEnd + "\"}");

        PartitionSlice partitionSlice = p.getPartitionTable().get("id1");
        assertEquals("range start should be all zeros", rangeStart, partitionSlice.getRangeMin());
        assertEquals("range end should be all f's", rangeEnd, partitionSlice.getRangeMax());

        PartitionSlice.ExpectedEntry expectedEntry = partitionSlice.getExpectedTable().get("1");
        assertEquals(activeResourceInfo, expectedEntry.getResourceInfo());
        assertEquals(true, expectedEntry.getActive());
        routerSem.acquire();
        verify(metricRouter).route(expectedEntry, KafkaMessageType.METRIC);

        p.stop("id1", "{" +
                "\"start\":\"" + rangeStart + "\"," +
                "\"end\":\"" + rangeEnd + "\"}");

        assertEquals(p.getPartitionTable().size(), 0);
        assertEquals(partitionSlice.getExpectedWatch().getRunning(), false);
        assertEquals(partitionSlice.getActiveWatch().getRunning(), false);
    }

    @Test
    public void testProcessorWatchConsumers() throws Exception {
        doNothing().when(metricRouter).route(any(), any());

        PresenceMonitorProcessor p = new PresenceMonitorProcessor(client, objectMapper,
                envoyResourceManagement, taskScheduler, metricExporter,
                new SimpleMeterRegistry());


        // wrap expected watch consumer to release a semaphore when done
        Semaphore expectedSem = new Semaphore(0);
        BiConsumer<WatchResponse, PartitionSlice> originalExpectedConsumer = p.getExpectedWatchResponseConsumer();
        BiConsumer<WatchResponse, PartitionSlice> newExpectedConsumer = (wr, pe) -> {
            originalExpectedConsumer.accept(wr, pe);
            expectedSem.release();
        };
        p.setExpectedWatchResponseConsumer(newExpectedConsumer);

        // wrap active watch consumer to release a semaphore when done
        Semaphore activeSem = new Semaphore(0);
        BiConsumer<WatchResponse, PartitionSlice> originalActiveConsumer = p.getActiveWatchResponseConsumer();
        BiConsumer<WatchResponse, PartitionSlice> newActiveConsumer = (wr, pe) -> {
            originalActiveConsumer.accept(wr, pe);
            activeSem.release();
        };
        p.setActiveWatchResponseConsumer(newActiveConsumer);


        p.start("id1", "{" +
                "\"start\":\"" + rangeStart + "\"," +
                "\"end\":\"" + rangeEnd + "\"}");

        PartitionSlice partitionSlice = p.getPartitionTable().get("id1");
        assertEquals("No resources exist yet so expected table should be empty",
                partitionSlice.getExpectedTable().size(), 0);

        // Now generate an expected watch and wait for the sem
        client.getKVClient().put(
                ByteSequence.fromString("/resources/expected/1"),
                ByteSequence.fromString(expectedResourceInfoString));
        expectedSem.acquire();

        assertEquals("The watch should have added one entry",
                partitionSlice.getExpectedTable().size(), 1);
        assertEquals("Entry should be inactive",
                partitionSlice.getExpectedTable().get("1").getActive(), false);
        assertEquals(expectedResourceInfo,
                partitionSlice.getExpectedTable().get("1").getResourceInfo());


        // Now generate an active watch and wait for the sem
        client.getKVClient().put(
                ByteSequence.fromString("/resources/active/1"),
                ByteSequence.fromString(activeResourceInfoString));
        activeSem.acquire();

        assertEquals("Entry should be active",
                partitionSlice.getExpectedTable().get("1").getActive(), true);
        assertEquals(activeResourceInfo,
                partitionSlice.getExpectedTable().get("1").getResourceInfo());
        verify(metricRouter).route(partitionSlice.getExpectedTable().get("1"), KafkaMessageType.EVENT);

        // Now delete active entry and see it go inactive
        client.getKVClient().delete(ByteSequence.fromString("/resources/active/1"));
        activeSem.acquire();

        assertEquals("Entry should be inactive",
                partitionSlice.getExpectedTable().get("1").getActive(), false);
        assertEquals(activeResourceInfo,
                partitionSlice.getExpectedTable().get("1").getResourceInfo());

        // Now delete expected entry and see it removed from the table
        client.getKVClient().delete(ByteSequence.fromString("/resources/expected/1"));
        expectedSem.acquire();

        assertEquals("Entry should be gone",
                partitionSlice.getExpectedTable().containsKey("1"), false);
    }
}
