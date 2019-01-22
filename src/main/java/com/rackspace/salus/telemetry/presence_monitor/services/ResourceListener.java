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

import com.rackspace.salus.telemetry.messaging.OperationType;
import com.rackspace.salus.telemetry.messaging.ResourceEvent;
import com.rackspace.salus.telemetry.model.ResourceInfo;
import com.rackspace.salus.telemetry.presence_monitor.types.PartitionSlice;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ResourceListener implements ConsumerSeekAware {
    ConcurrentHashMap<String, PartitionSlice> partitionTable;

    @Autowired
    public ResourceListener(ConcurrentHashMap<String, PartitionSlice> partitionTable) {
        this.partitionTable = partitionTable;
    }

    @KafkaListener(topics = "${presence-monitor.kafka-topics.RESOURCE}")
    public void resourceListener(ConsumerRecord<String, ResourceEvent> record) {
        boolean keyFound = false;
        for (Map.Entry<String, PartitionSlice> e : partitionTable.entrySet()) {
            PartitionSlice slice = e.getValue();
            if ((record.key().compareTo(slice.getRangeMin()) >= 0) &&
                    (record.key().compareTo(slice.getRangeMax()) <= 0)) {
                log.trace("record {} used to update slice", record.key());
                keyFound = true;
                updateSlice(slice, record.key(), record.value());
                break;
            }
        }
        if (!keyFound) {
            log.trace("record {} ignored", record.key());
        }
    }

    // synchronized to prevent slice from being updated simultaneously when a new slice is added
    protected synchronized void updateSlice(PartitionSlice slice, String key, ResourceEvent resourceEvent) {
        boolean enabled = resourceEvent.getResource().getPresenceMonitoringEnabled();
        ResourceInfo rinfo = PresenceMonitorProcessor.convert(resourceEvent.getResource());
        if (!(resourceEvent.getOperation().equals(OperationType.DELETE)) && enabled) {
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
