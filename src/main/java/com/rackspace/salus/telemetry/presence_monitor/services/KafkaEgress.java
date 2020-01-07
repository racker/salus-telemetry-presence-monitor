/*
 * Copyright 2020 Rackspace US, Inc.
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

import com.rackspace.salus.common.errors.RuntimeKafkaException;
import com.rackspace.salus.common.messaging.KafkaTopicProperties;
import com.rackspace.salus.telemetry.messaging.KafkaMessageType;
import java.util.concurrent.ExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaEgress {

    private final KafkaTemplate kafkaTemplate;
    private final KafkaTopicProperties kafkaTopicProperties;

    @Autowired
    public KafkaEgress(KafkaTemplate kafkaTemplate, KafkaTopicProperties kafkaTopicProperties) {
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaTopicProperties = kafkaTopicProperties;
    }

    public void send(String tenantId, KafkaMessageType messageType, String payload) {
        final String topic;
        switch (messageType) {
            case METRIC:
                topic = kafkaTopicProperties.getMetrics();
                break;
            default:
                throw new IllegalArgumentException("Unsupported messageType: " + messageType);
        }

        if (topic == null) {
            throw new IllegalArgumentException(String.format("No topic configured for %s", messageType));
        }
        try {
            kafkaTemplate.send(topic, tenantId, payload).get();
        } catch (InterruptedException|ExecutionException e) {
            throw new RuntimeKafkaException(e);
        }
    }
}
