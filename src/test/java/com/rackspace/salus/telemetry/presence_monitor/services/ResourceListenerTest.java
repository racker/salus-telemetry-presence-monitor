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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.salus.common.messaging.KafkaTopicProperties;
import com.rackspace.salus.common.util.KeyHashing;
import com.rackspace.salus.telemetry.messaging.ResourceEvent;
import com.rackspace.salus.telemetry.model.Resource;
import com.rackspace.salus.telemetry.presence_monitor.config.PresenceMonitorProperties;
import com.rackspace.salus.telemetry.presence_monitor.types.PartitionSlice;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
@Slf4j
@Import({PresenceMonitorProperties.class})
@ActiveProfiles("test")
public class ResourceListenerTest {
    private KeyHashing hashing = new KeyHashing();

    private static String sliceKey = "id1";

    @MockBean
    RestTemplateBuilder restTemplateBuilder;

    @Mock
    RestTemplate restTemplate;

    @Mock
    ResponseEntity<Resource> responseEntity;

    @Autowired
    PresenceMonitorProperties props;

    private static ConcurrentHashMap<String, PartitionSlice>
            partitionTable;

    private String resourceString =
            "{\"resourceId\":\"os:LINUX\"," +
                    "\"labels\":{\"os\":\"LINUX\",\"arch\":\"X86_64\"},\"id\":1," +
                    "\"presenceMonitoringEnabled\":true," +
                    "\"tenantId\":\"123456\"}";
    private String updatedResourceString = resourceString.replaceAll("X86_64", "X86_32");

    private String tenantId = "t1";
    private String resourceId = "r1";

    private ResourceEvent resourceEvent = new ResourceEvent();

    private Resource resource, updatedResource;
    private ObjectMapper objectMapper = new ObjectMapper();

    @Before
    public void setUp() throws Exception {
        String rangeStart = "0000000000000000000000000000000000000000000000000000000000000000",
               rangeEnd = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
        partitionTable = new ConcurrentHashMap<>();
        PartitionSlice slice = new PartitionSlice();
        slice.setRangeMin(rangeStart);
        slice.setRangeMax(rangeEnd);
        partitionTable.put(sliceKey, slice);
        resource = objectMapper.readValue(resourceString, Resource.class);
        updatedResource = objectMapper.readValue(updatedResourceString, Resource.class);
        resourceEvent.setResourceId(resourceId).setTenantId(tenantId);
    }

    @Test
    public void testListener() {
        String key = String.format("%s:%s", tenantId, resourceId);
        String hash = hashing.hash(key);
        when(restTemplateBuilder.build()).thenReturn(restTemplate);
        when(restTemplate.getForEntity(anyString(),any(), (Map)any())).thenReturn(responseEntity);
        when(responseEntity.getStatusCode()).thenReturn(HttpStatus.OK);
        when(responseEntity.getBody()).thenReturn(resource);


        ResourceListener rl = new ResourceListener(partitionTable, new KafkaTopicProperties(), restTemplateBuilder, props);
        ConsumerRecord<String, ResourceEvent> cr = new ConsumerRecord<>("http://dummy", 0, 0, key, resourceEvent);
        // send the message
        assertNull("Confirm no entry", partitionTable.get(sliceKey).getExpectedTable().get(hash));
        rl.resourceListener(cr);
        PartitionSlice.ExpectedEntry entry = partitionTable.get(sliceKey).getExpectedTable().get(hash);
        assertEquals("Confirm new entry", entry.getResourceInfo(), PresenceMonitorProcessor.convert(resource));

        when(responseEntity.getBody()).thenReturn(updatedResource);
        rl.resourceListener(cr);
        entry = partitionTable.get(sliceKey).getExpectedTable().get(hash);
        assertEquals("Confirm updated entry", entry.getResourceInfo(), PresenceMonitorProcessor.convert(updatedResource));

        // Throwing not found exception should be interpreted as the resource should be deleted
        when(restTemplate.getForEntity(anyString(),any(), (Map)any())).thenThrow(new HttpClientErrorException(HttpStatus.NOT_FOUND));
        rl.resourceListener(cr);
        assertNull("Confirm deleted entry", partitionTable.get(sliceKey).getExpectedTable().get(hash));
    }

}
