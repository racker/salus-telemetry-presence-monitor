package com.rackspace.salus.telemetry.presence_monitor.services;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.watch.WatchResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.salus.telemetry.etcd.config.KeyHashing;
import com.rackspace.salus.telemetry.etcd.services.EnvoyResourceManagement;
import com.rackspace.salus.telemetry.model.Resource;
import com.rackspace.salus.telemetry.model.ResourceInfo;
import com.rackspace.salus.telemetry.presence_monitor.config.PresenceMonitorProperties;
import com.rackspace.salus.telemetry.presence_monitor.types.KafkaMessageType;
import com.rackspace.salus.telemetry.presence_monitor.types.PartitionSlice;
import io.etcd.jetcd.launcher.junit.EtcdClusterResource;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import jdk.internal.util.xml.impl.Input;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.*;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.client.ResponseExtractor;
import org.springframework.web.client.RestTemplate;

@RunWith(SpringRunner.class)
@EnableAutoConfiguration
@ComponentScan({"PresenceMonitorProperties.class"})
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = {
        "presence-monitor.resourceManagerUrl=xxyyzz",
        "presence-monitor.export-period=4",
        "presence-monitor.kafka-topics.LOG=telemetry.logs.json",
        "presence-monitor.kafka.bootstrap.servers=localhost:9092",
        "presence-monitor.kafka.group.id=presence.monitor",
        "presence-monitor.kafka.value.deserializer: org.springframework.kafka.support.serializer.JsonDeerializer"
    }
)

@ActiveProfiles("test")
public class PresenceMonitorProcessorTest {
    @Configuration
    @Import({KeyHashing.class, MetricExporter.class, PresenceMonitorProperties.class, RestTemplate.class})
    public static class TestConfig {
        @Bean
        MeterRegistry getMeterRegistry() {
            return new SimpleMeterRegistry();
        }
    }

    @Rule
    public final EtcdClusterResource etcd = new EtcdClusterResource("PresenceMonitorProcessorTest", 1);

    private ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    MetricExporter metricExporter;

    @MockBean
    MetricRouter metricRouter;

    @Autowired
    SimpleMeterRegistry simpleMeterRegistry;

    private ThreadPoolTaskScheduler taskScheduler;

    private EnvoyResourceManagement envoyResourceManagement;

    private Client client;

    @Autowired
    KeyHashing hashing;

    @MockBean
    RestTemplate restTemplate;
    
    private String expectedResourceString =
            "{\"resourceIdentifier\":{\"identifierName\":\"os\",\"identifierValue\":\"LINUX\"}," +
                    "\"labels\":{\"os\":\"LINUX\",\"arch\":\"X86_64\"},\"id\":1," +
                    "\"tenantId\":\"123456\"}";

    private Resource expectedResource;

    private String activeResourceInfoString;

    private ResourceInfo expectedResourceInfo;

    private ResourceInfo activeResourceInfo;

    private String rangeStart = "0000000000000000000000000000000000000000000000000000000000000000",
            rangeEnd = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";

    //@Autowired
    //PresenceMonitorProcessorTest(PresenceMonitorProperties props) {
    //    presenceMonitorProperties = props;
    //}
    @Autowired
    private PresenceMonitorProperties presenceMonitorProperties;

    @Mock
    ClientHttpResponse response;

    String expectedId;
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
        expectedResource = objectMapper.readValue(expectedResourceString, Resource.class);
        expectedResourceInfo = PresenceMonitorProcessor.convert(expectedResource);
        activeResourceInfoString = objectMapper.writeValueAsString(expectedResourceInfo).replace("X86_64", "X86_32");

        activeResourceInfo = objectMapper.readValue(activeResourceInfoString, ResourceInfo.class);
        presenceMonitorProperties.setExportPeriod(Duration.ofSeconds(1));
        presenceMonitorProperties.setResourceManagerUrl("http://localhost/getResources");
        metricExporter = new MetricExporter(metricRouter, presenceMonitorProperties, simpleMeterRegistry);
    }


    @Test
    public void testProcessorStart() throws Exception {
        InputStream testStream = new ByteArrayInputStream((PresenceMonitorProcessor.SSEHdr + " " + expectedResourceString + "\n\n").getBytes());
        when(response.getBody()).thenReturn(testStream);
        doAnswer(invocation ->{
            ResponseExtractor<InputStream> responseExtractor = invocation.getArgument(3);
            return responseExtractor.extractData(response);
        }).when(restTemplate).execute(any(), any(), any(), any(), (Object)any());


        Semaphore routerSem = new Semaphore(0);
        doAnswer((a) -> {
            routerSem.release();
            return null;
        }).when(metricRouter).route(any(), any());

        PresenceMonitorProcessor p = new PresenceMonitorProcessor(client, objectMapper,
                envoyResourceManagement, taskScheduler, metricExporter,
                simpleMeterRegistry, hashing, presenceMonitorProperties, restTemplate);

        expectedId = p.genExpectedId(expectedResourceInfo);
        client.getKVClient().put(
                ByteSequence.fromString("/resources/active/" + expectedId),
                ByteSequence.fromString(activeResourceInfoString)).join();

        p.start("id1", "{" +
                "\"start\":\"" + rangeStart + "\"," +
                "\"end\":\"" + rangeEnd + "\"}");

        PartitionSlice partitionSlice = p.getPartitionTable().get("id1");
        assertEquals("range start should be all zeros", rangeStart, partitionSlice.getRangeMin());
        assertEquals("range end should be all f's", rangeEnd, partitionSlice.getRangeMax());

        PartitionSlice.ExpectedEntry expectedEntry = partitionSlice.getExpectedTable().get(expectedId);
        assertEquals(activeResourceInfo, expectedEntry.getResourceInfo());
        assertEquals(true, expectedEntry.getActive());
        routerSem.acquire();
        verify(metricRouter).route(expectedEntry, KafkaMessageType.METRIC);

        p.stop("id1", "{" +
                "\"start\":\"" + rangeStart + "\"," +
                "\"end\":\"" + rangeEnd + "\"}");

        assertEquals(p.getPartitionTable().size(), 0);
        assertEquals(partitionSlice.getActiveWatch().getRunning(), false);
    }

//    @Test
    // public void testProcessorWatchConsumers() throws Exception {
    //     doNothing().when(metricRouter).route(any(), any());

    //     PresenceMonitorProcessor p = new PresenceMonitorProcessor(client, objectMapper,
    //             envoyResourceManagement, taskScheduler, metricExporter,
    //             new SimpleMeterRegistry(), hashing, presenceMonitorProperties, restTemplate);


    //     // wrap active watch consumer to release a semaphore when done
    //     Semaphore activeSem = new Semaphore(0);
    //     BiConsumer<WatchResponse, PartitionSlice> originalActiveConsumer = p.getActiveWatchResponseConsumer();
    //     BiConsumer<WatchResponse, PartitionSlice> newActiveConsumer = (wr, pe) -> {
    //         originalActiveConsumer.accept(wr, pe);
    //         activeSem.release();
    //     };
    //     p.setActiveWatchResponseConsumer(newActiveConsumer);


    //     p.start("id1", "{" +
    //             "\"start\":\"" + rangeStart + "\"," +
    //             "\"end\":\"" + rangeEnd + "\"}");

    //     PartitionSlice partitionSlice = p.getPartitionTable().get("id1");
    //     assertEquals("No resources exist yet so expected table should be empty",
    //             partitionSlice.getExpectedTable().size(), 0);

    //     // Now generate an expected watch and wait for the sem
    //     client.getKVClient().put(
    //             ByteSequence.fromString("/resources/expected/1"),
    //             ByteSequence.fromString(expectedResourceInfoString));
    //     expectedSem.acquire();

    //     assertEquals("The watch should have added one entry",
    //             partitionSlice.getExpectedTable().size(), 1);
    //     assertEquals("Entry should be inactive",
    //             partitionSlice.getExpectedTable().get("1").getActive(), false);
    //     assertEquals(expectedResourceInfo,
    //             partitionSlice.getExpectedTable().get("1").getResourceInfo());


    //     // Now generate an active watch and wait for the sem
    //     client.getKVClient().put(
    //             ByteSequence.fromString("/resources/active/1"),
    //             ByteSequence.fromString(activeResourceInfoString));
    //     activeSem.acquire();

    //     assertEquals("Entry should be active",
    //             partitionSlice.getExpectedTable().get("1").getActive(), true);
    //     assertEquals(activeResourceInfo,
    //             partitionSlice.getExpectedTable().get("1").getResourceInfo());
    //     verify(metricRouter).route(partitionSlice.getExpectedTable().get("1"), KafkaMessageType.EVENT);

    //     // Now delete active entry and see it go inactive
    //     client.getKVClient().delete(ByteSequence.fromString("/resources/active/1"));
    //     activeSem.acquire();

    //     assertEquals("Entry should be inactive",
    //             partitionSlice.getExpectedTable().get("1").getActive(), false);
    //     assertEquals(activeResourceInfo,
    //             partitionSlice.getExpectedTable().get("1").getResourceInfo());

    //     // Now delete expected entry and see it removed from the table
    //     client.getKVClient().delete(ByteSequence.fromString("/resources/expected/1"));
    //     expectedSem.acquire();

    //     assertEquals("Entry should be gone",
    //             partitionSlice.getExpectedTable().containsKey("1"), false);
    // }
}
