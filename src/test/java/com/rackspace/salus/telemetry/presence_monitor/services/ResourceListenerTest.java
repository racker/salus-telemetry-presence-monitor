/*
 *    Copyright 2019 Rackspace US, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 *
 */

package com.rackspace.salus.telemetry.presence_monitor.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.salus.telemetry.messaging.OperationType;
import com.rackspace.salus.telemetry.messaging.ResourceEvent;
import com.rackspace.salus.telemetry.model.Resource;
import com.rackspace.salus.telemetry.presence_monitor.types.PartitionSlice;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@EnableAutoConfiguration
@DirtiesContext
@Slf4j
@ActiveProfiles("test")
public class ResourceListenerTest {

    @Configuration
    public static class TestConfig {
        @Bean
        ResourceListener getRL() {
            String rangeStart = "0000000000000000000000000000000000000000000000000000000000000000",
                    rangeEnd = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";

            partitionTable = new ConcurrentHashMap<>();
            PartitionSlice slice = new PartitionSlice();
            slice.setRangeMin(rangeStart);
            slice.setRangeMax(rangeEnd);
            partitionTable.put(sliceKey, slice);
            return new SliceUpdateListener(partitionTable);
        }
    }

    private static String sliceKey = "id1";

    @Value("${presence-monitor.kafka-topics.RESOURCE}")
    private String TOPIC;

    @Autowired
    private KafkaTemplate template;

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @ClassRule
    public static EmbeddedKafkaRule embeddedKafka =
            new EmbeddedKafkaRule(1, true, 1);

    private static ConcurrentHashMap<String, PartitionSlice>
            partitionTable;

    private String resourceString =
            "{\"resourceId\":\"os:LINUX\"," +
                    "\"labels\":{\"os\":\"LINUX\",\"arch\":\"X86_64\"},\"id\":1," +
                    "\"presenceMonitoringEnabled\":true," +
                    "\"tenantId\":\"123456\"}";
    private String updatedResourceString = resourceString.replaceAll("X86_64", "X86_32");

    private ResourceEvent resourceEvent = new ResourceEvent();
    private ResourceEvent updatedResourceEvent = new ResourceEvent();
    private Resource resource, updatedResource;
    private ObjectMapper objectMapper = new ObjectMapper();
    private static Semaphore listenerSem = new Semaphore(0);

    @Before
    public void setUp() throws Exception {
        resource = objectMapper.readValue(resourceString, Resource.class);
        updatedResource = objectMapper.readValue(updatedResourceString, Resource.class);
        resourceEvent.setResource(resource).setOperation(OperationType.CREATE);
        updatedResourceEvent.setResource(updatedResource).setOperation(OperationType.UPDATE);


        // wait until the partitions are assigned
        for (MessageListenerContainer messageListenerContainer : kafkaListenerEndpointRegistry
                .getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(messageListenerContainer,
                    embeddedKafka.getEmbeddedKafka().getPartitionsPerTopic());
        }


    }

    @Test
    public void testListener() throws Exception {
        String key = "00001";
        // send the message
        assertNull("Confirm no entry", partitionTable.get(sliceKey).getExpectedTable().get(key));
        template.send(TOPIC, key, resourceEvent);
        listenerSem.acquire();
        PartitionSlice.ExpectedEntry entry = partitionTable.get(sliceKey).getExpectedTable().get(key);
        assertEquals("Confirm new entry", entry.getResourceInfo(), PresenceMonitorProcessor.convert(resource));

        template.send(TOPIC, key, updatedResourceEvent);
        listenerSem.acquire();
        entry = partitionTable.get(sliceKey).getExpectedTable().get(key);
        assertEquals("Confirm updated entry", entry.getResourceInfo(), PresenceMonitorProcessor.convert(updatedResource));

        resourceEvent.setOperation(OperationType.DELETE);
        template.send(TOPIC, key, resourceEvent);
        listenerSem.acquire();
        assertNull("Confirm deleted entry", partitionTable.get(sliceKey).getExpectedTable().get(key));
    }

    static class SliceUpdateListener extends ResourceListener {
        SliceUpdateListener(Map<String, PartitionSlice> partitionTable) {
            super();
        }

        protected synchronized void updateSlice(PartitionSlice slice, String key, ResourceEvent resourceEvent) {
            super.updateSlice(slice, key, resourceEvent);
            listenerSem.release();
        }

    }
}
