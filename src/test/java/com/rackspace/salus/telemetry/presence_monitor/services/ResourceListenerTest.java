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
import com.rackspace.salus.resource_management.web.client.ResourceApi;
import com.rackspace.salus.telemetry.messaging.ResourceEvent;
import com.rackspace.salus.telemetry.model.Resource;
import com.rackspace.salus.telemetry.presence_monitor.types.PartitionSlice;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
@Slf4j
@ActiveProfiles("test")
public class ResourceListenerTest {
    private KeyHashing hashing = new KeyHashing();

    private static String sliceKey = "id1";

    @Mock
    ResourceApi resourceApi;

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

        ResourceListener rl = new ResourceListener(partitionTable, new KafkaTopicProperties(), resourceApi);
        ConsumerRecord<String, ResourceEvent> cr = new ConsumerRecord<>("http://dummy", 0, 0, key, resourceEvent);
        assertNull("Confirm no entry", partitionTable.get(sliceKey).getExpectedTable().get(hash));

        // send the initial message
        when(resourceApi.getByResourceId(any(), any())).thenReturn(resource);
        rl.handleResourceEvent(cr);
        PartitionSlice.ExpectedEntry entry = partitionTable.get(sliceKey).getExpectedTable().get(hash);
        assertEquals("Confirm new entry", entry.getResourceInfo(), PresenceMonitorProcessor.convert(resource));

        // send a resource event due to an update to the resource
        when(resourceApi.getByResourceId(any(), any())).thenReturn(updatedResource);
        rl.handleResourceEvent(cr);
        entry = partitionTable.get(sliceKey).getExpectedTable().get(hash);
        assertEquals("Confirm updated entry", entry.getResourceInfo(), PresenceMonitorProcessor.convert(updatedResource));

        // Throwing not found exception should be interpreted as the resource should be deleted
        when(resourceApi.getByResourceId(any(), any())).thenReturn(null);
        rl.handleResourceEvent(cr);
        assertNull("Confirm deleted entry", partitionTable.get(sliceKey).getExpectedTable().get(hash));
    }

}
