/*
 * Copyright 2019 Rackspace US, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import com.rackspace.salus.common.messaging.EnableSalusKafkaMessaging;
import com.rackspace.salus.common.messaging.KafkaTopicProperties;
import com.rackspace.salus.common.util.KeyHashing;
import com.rackspace.salus.resource_management.web.client.ResourceApi;
import com.rackspace.salus.telemetry.etcd.services.EnvoyResourceManagement;
import com.rackspace.salus.telemetry.messaging.KafkaMessageType;
import com.rackspace.salus.telemetry.model.Resource;
import com.rackspace.salus.telemetry.model.ResourceInfo;
import com.rackspace.salus.telemetry.presence_monitor.config.PresenceMonitorProperties;
import com.rackspace.salus.telemetry.presence_monitor.types.PartitionSlice;
import io.etcd.jetcd.launcher.junit.EtcdClusterResource;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {PresenceMonitorProcessorTest.TOPIC_METRICS})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@EnableSalusKafkaMessaging
public class PresenceMonitorProcessorTest {

    public static final String TOPIC_METRICS = "test.metrics.json";

    // Declare our own unit test Spring config, which is merged with the main app config.
    @TestConfiguration
    public static class TestConfig {

        // Adjust our standard topic properties to point metrics at our test topic
        @Bean
        public KafkaTopicProperties kafkaTopicProperties() {
            final KafkaTopicProperties properties = new KafkaTopicProperties();
            properties.setMetrics(TOPIC_METRICS);
            return properties;
        }

        @Bean
        @Primary // This overrides any existing Client bean with this bean.
        public Client client() {
            final List<String> endpoints = etcd.cluster().getClientEndpoints().stream()
                    .map(URI::toString)
                    .collect(Collectors.toList());
            return com.coreos.jetcd.Client.builder().endpoints(endpoints).build();
        }
    }

    @ClassRule
    public static final EtcdClusterResource etcd = new EtcdClusterResource("PresenceMonitorProcessorTest", 1);

    private ObjectMapper objectMapper = new ObjectMapper();

    @MockBean
    MetricExporter metricExporter;

    @Mock
    private MetricRouter metricRouter;

    @Autowired
    SimpleMeterRegistry simpleMeterRegistry;

    @Autowired
    KeyHashing hashing;

    private ThreadPoolTaskScheduler taskScheduler;

    private EnvoyResourceManagement envoyResourceManagement;

    @Autowired
    private Client client;

    private String expectedResourceString =
            "{\"resourceId\":\"os:LINUX\"," +
                    "\"labels\":{\"os\":\"LINUX\",\"arch\":\"X86_64\"},\"id\":1," +
                    "\"presenceMonitoringEnabled\":true," +
                    "\"tenantId\":\"123456\"}";

    private String activeResourceInfoString;

    private Resource expectedResource;

    private ResourceInfo expectedResourceInfo;

    private ResourceInfo activeResourceInfo;

    private String rangeStart = "0000000000000000000000000000000000000000000000000000000000000000",
            rangeEnd = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";

    @Autowired
    private PresenceMonitorProperties presenceMonitorProperties;

    @Mock
    ResourceApi resourceApi;

    @Autowired
    ConcurrentHashMap<String, PartitionSlice> partitionTable;

    @Autowired
    @Qualifier("resourceListener")
    ResourceListener resourceListener;

    @Before
    public void setUp() throws Exception {
        taskScheduler = new ThreadPoolTaskScheduler();
        taskScheduler.setPoolSize(Integer.MAX_VALUE);
        taskScheduler.setThreadNamePrefix("tasks-");
        taskScheduler.initialize();

        envoyResourceManagement = new EnvoyResourceManagement(client, objectMapper, hashing);
        expectedResource = objectMapper.readValue(expectedResourceString, Resource.class);
        expectedResourceInfo = PresenceMonitorProcessor.convert(expectedResource);
        activeResourceInfoString = objectMapper.writeValueAsString(expectedResourceInfo).replace("X86_64", "X86_32");

        activeResourceInfo = objectMapper.readValue(activeResourceInfoString, ResourceInfo.class);
    }


    @Test
    public void testProcessorStart() throws Exception {
        MetricExporter metricExporter = new MetricExporter(metricRouter, presenceMonitorProperties, simpleMeterRegistry);

        when(resourceApi.getExpectedEnvoys()).thenReturn(Collections.singletonList(expectedResource));

        Semaphore routerSem = new Semaphore(0);
        doAnswer((a) -> {
            routerSem.release();
            return null;
        }).when(metricRouter).route(any(), any());

        PresenceMonitorProcessor p = new PresenceMonitorProcessor(client, objectMapper,
                envoyResourceManagement, taskScheduler, metricExporter,
                simpleMeterRegistry, resourceListener, partitionTable, resourceApi);

        String expectedId = PresenceMonitorProcessor.genExpectedId(expectedResourceInfo.getTenantId(),
                expectedResourceInfo.getResourceId());
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

    @Test
    public void testProcessorWatchConsumers() throws Exception {
        doNothing().when(metricRouter).route(any(), any());

        MetricExporter metricExporter = new MetricExporter(metricRouter, presenceMonitorProperties, simpleMeterRegistry);
        PresenceMonitorProcessor p = new PresenceMonitorProcessor(client, objectMapper,
                envoyResourceManagement, taskScheduler, metricExporter,
                new SimpleMeterRegistry(), resourceListener, partitionTable, resourceApi);

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

        PartitionSlice partitionSlice = p.
                getPartitionTable().get("id1");
        assertEquals("No resources exist yet so expected table should be empty",
                partitionSlice.getExpectedTable().size(), 0);

        // Now generate an active watch and wait for the sem
        String activeId = PresenceMonitorProcessor.genExpectedId(activeResourceInfo.getTenantId(),
                activeResourceInfo.getResourceId());
        client.getKVClient().put(
                ByteSequence.fromString("/resources/active/" + activeId),
                ByteSequence.fromString(activeResourceInfoString));
        activeSem.acquire();

        assertEquals("Entry should be active",
                partitionSlice.getExpectedTable().get(activeId).getActive(), true);
        assertEquals(activeResourceInfo,
                partitionSlice.getExpectedTable().get(activeId).getResourceInfo());
        verify(metricRouter).route(partitionSlice.getExpectedTable().get(activeId), KafkaMessageType.EVENT);

        // Now delete active entry and see it go inactive
        client.getKVClient().delete(ByteSequence.fromString("/resources/active/" + activeId));
        activeSem.acquire();

        assertEquals("Entry should be inactive",
                partitionSlice.getExpectedTable().get(activeId).getActive(), false);
        assertEquals(activeResourceInfo,
                partitionSlice.getExpectedTable().get(activeId).getResourceInfo());
    }
}
