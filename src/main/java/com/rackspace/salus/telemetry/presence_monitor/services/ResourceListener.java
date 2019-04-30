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

import com.rackspace.salus.common.messaging.KafkaTopicProperties;
import com.rackspace.salus.resource_management.web.client.ResourceApi;
import com.rackspace.salus.telemetry.messaging.ResourceEvent;
import com.rackspace.salus.telemetry.model.Resource;
import com.rackspace.salus.telemetry.model.ResourceInfo;
import com.rackspace.salus.telemetry.presence_monitor.types.PartitionSlice;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;


@Slf4j
@Component
public class ResourceListener implements ConsumerSeekAware {

    private final String topic;
    private ConcurrentHashMap<String, PartitionSlice> partitionTable;
    private final ResourceApi resourceApi;

    @Autowired
    public ResourceListener(ConcurrentHashMap<String, PartitionSlice> partitionTable,
                            KafkaTopicProperties kafkaTopicProperties, ResourceApi resourceApi) {
        this.partitionTable = partitionTable;
        this.topic = kafkaTopicProperties.getResources();
        this.resourceApi = resourceApi;
    }

    public String getTopic() {
        return topic;
    }

    @KafkaListener(topics = "#{__listener.topic}")
    public void handleResourceEvent(ConsumerRecord<String, ResourceEvent> record) {
        ResourceEvent event = record.value();
        Resource resource = resourceApi.getByResourceId(event.getTenantId(), event.getResourceId());
        ResourceInfo rinfo = PresenceMonitorProcessor.convert(resource);

        String hash = PresenceMonitorProcessor.genExpectedId(event.getTenantId(), event.getResourceId());
        boolean keyFound = false;
        for (Map.Entry<String, PartitionSlice> e : partitionTable.entrySet()) {
            PartitionSlice slice = e.getValue();
            if (PresenceMonitorProcessor.sliceContains(slice, hash)) {
                log.trace("record {} used to update slice", record.key());
                keyFound = true;
                updateSlice(slice, hash, resource, rinfo);
                break;
            }
        }
        if (!keyFound) {
            log.trace("record {} ignored", record.key());
        }
    }

    // synchronized to prevent slice from being updated simultaneously when a new slice is added
    synchronized void updateSlice(PartitionSlice slice, String key, Resource resource, ResourceInfo rinfo) {
        if (resource != null && resource.getPresenceMonitoringEnabled()) {
            PartitionSlice.ExpectedEntry newEntry = new PartitionSlice.ExpectedEntry();
            PartitionSlice.ExpectedEntry oldEntry = slice.getExpectedTable().get(key);
            newEntry.setResourceInfo(rinfo);
            if (oldEntry != null) {
                newEntry.setActive(oldEntry.getActive());
            } else {
                newEntry.setActive(false);
            }
            slice.getExpectedTable().put(key, newEntry);

        } else {
            slice.getExpectedTable().remove(key);
        }
    }

    @Override
    public void registerSeekCallback(ConsumerSeekCallback consumerSeekCallback) {
        // do nothing
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> map, ConsumerSeekCallback consumerSeekCallback) {
        for (Map.Entry<TopicPartition, Long> e : map.entrySet()) {
            consumerSeekCallback.seekToEnd(e.getKey().topic(), e.getKey().partition());
        }
    }

    @Override
    public void onIdleContainer(Map<TopicPartition, Long> map, ConsumerSeekCallback consumerSeekCallback) {
        // do nothing
    }
}
