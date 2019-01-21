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

package com.rackspace.salus.telemetry.presence_monitor.config;


import com.rackspace.salus.telemetry.messaging.ResourceEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
@Slf4j
public class KafkaConsumerConfig {
    @Value("${presence-monitor.kafka.bootstrap-servers}")
    String servers;
    @Value("${presence-monitor.kafka.group-id}")
    String groupId;
    @Bean
    public ConsumerFactory<String, ResourceEvent> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
          servers);
        props.put(
          ConsumerConfig.GROUP_ID_CONFIG, 
          groupId);
        props.put(
          ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
          StringDeserializer.class);
        props.put(
          ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
          JsonDeserializer.class);
      return new DefaultKafkaConsumerFactory<>(
        props,
        new StringDeserializer(), 
        new JsonDeserializer<>(ResourceEvent.class));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, ResourceEvent>
      kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, ResourceEvent> factory
          = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }
}
