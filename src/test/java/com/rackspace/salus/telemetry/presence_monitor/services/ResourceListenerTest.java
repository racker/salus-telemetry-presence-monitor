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
import com.rackspace.salus.telemetry.messaging.ResourceEvent;
import com.rackspace.salus.telemetry.model.Resource;
import com.rackspace.salus.telemetry.presence_monitor.config.KafkaConsumerConfig;
import com.rackspace.salus.telemetry.presence_monitor.types.PartitionSlice;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
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
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@EnableAutoConfiguration
@DirtiesContext
@Slf4j
public class ResourceListenerTest {

  @Configuration
  @Import({KafkaConsumerConfig.class})
  public static class TestConfig {
    @Bean
    ResourceListener getRL() {
      partitionTable = new ConcurrentHashMap<>();
      PartitionSlice slice = new PartitionSlice();
      slice.setRangeMin(rangeStart);
      slice.setRangeMax(rangeEnd);
      partitionTable.put("id1", slice);
      return new ResourceListener(partitionTable);
    }
  }



  // @Value("${presence-monitor.kafka-topics.RESOURCE}")
  private static String TOPIC = "telemetry.resources.json";

  @Autowired
  private ResourceListener resourceListener;

  private KafkaTemplate<String, ResourceEvent> template;

  @Autowired
  private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

  @ClassRule
  public static EmbeddedKafkaRule embeddedKafka =
      new EmbeddedKafkaRule(1, true, 1, TOPIC);

  private static ConcurrentHashMap<String, PartitionSlice>
          partitionTable;

  private String resourceString =
          "{\"resourceIdentifier\":{\"identifierName\":\"os\",\"identifierValue\":\"LINUX\"}," +
                  "\"labels\":{\"os\":\"LINUX\",\"arch\":\"X86_64\"},\"id\":1," +
                  "\"tenantId\":\"123456gbj\"}";

  private ResourceEvent resourceEvent = new ResourceEvent();
  private Resource resource;
  private ObjectMapper objectMapper = new ObjectMapper();

  private static String rangeStart = "0000000000000000000000000000000000000000000000000000000000000000",
          rangeEnd = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";

  @Before
  public void setUp() throws Exception {
    log.info("gbjrlserver " +       embeddedKafka.getEmbeddedKafka().getBrokersAsString());
    resource = objectMapper.readValue(resourceString, Resource.class);
    resourceEvent.setResource(resource).setOperation("create");
    // set up the Kafka producer properties
    Map<String, Object> props = new HashMap<>();
    props.put(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
//      "localhost:9092");

      embeddedKafka.getEmbeddedKafka().getBrokersAsString());
    props.put(
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
      StringSerializer.class);
    props.put(
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
      JsonSerializer.class);
    // create a Kafka producer factory
    ProducerFactory<String, ResourceEvent> producerFactory =
        new DefaultKafkaProducerFactory<String, ResourceEvent>(

            props);

    // create a Kafka template
    template = new KafkaTemplate<>(producerFactory);
    // set the default topic to send to
    template.setDefaultTopic(TOPIC);

    // wait until the partitions are assigned
    System.out.println("gbj wait");
    for (MessageListenerContainer messageListenerContainer : kafkaListenerEndpointRegistry
        .getListenerContainers()) {
      ContainerTestUtils.waitForAssignment(messageListenerContainer,
          embeddedKafka.getEmbeddedKafka().getPartitionsPerTopic());
    }
  }

  @Test
  public void testListener() throws Exception {
    // send the message
    System.out.println("gbj start");
    ListenableFuture<SendResult<String, ResourceEvent>> res = template.send(TOPIC, "00001", resourceEvent);
    SendResult<String , ResourceEvent> sr = res.get();
    System.out.println("gbj done");
  }
}
