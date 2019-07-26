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

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.salus.telemetry.etcd.EtcdUtils;
import com.rackspace.salus.telemetry.messaging.KafkaMessageType;
import com.rackspace.salus.telemetry.presence_monitor.types.PartitionSlice;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.launcher.junit.EtcdClusterResource;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import org.apache.avro.io.EncoderFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
public class MetricRouterTest {

    // Configure a bare bones app context
    @Configuration
    public static class TestConfig {

      @Bean
      public EncoderFactory encoderFactory() {
        return new EncoderFactory();
      }
    }

    @Rule
    public final EtcdClusterResource etcd = new EtcdClusterResource("MetricRouterTest", 1);

    private ObjectMapper objectMapper = new ObjectMapper();

    @MockBean
    KafkaEgress kafkaEgress;

    @MockBean
    TimestampProvider timestampProvider;

    private MetricRouter metricRouter;

    @Autowired
            EncoderFactory encoderFactory;

    private Client client;

    private PartitionSlice.ExpectedEntry expectedEntry;
    @Before
    public void setUp() throws Exception {
      client = io.etcd.jetcd.Client.builder().endpoints(
          etcd.cluster().getClientEndpoints()
      ).build();

      when(timestampProvider.getCurrentInstant())
          .thenReturn(Instant.EPOCH);

      metricRouter = new MetricRouter(encoderFactory, kafkaEgress, client, objectMapper, new SimpleMeterRegistry(), timestampProvider);
        String expectedEntryString = "{\"active\": true, \"resourceInfo\":{\"resourceId\":\"os:LINUX\"," +
                "\"labels\":{\"os\":\"LINUX\",\"arch\":\"X86_32\"},\"envoyId\":\"abcde\"," +
                "\"tenantId\":\"123456\",\"address\":\"host:1234\"}}";
        expectedEntry = objectMapper.readValue(expectedEntryString, PartitionSlice.ExpectedEntry.class);

    }

    @Test
    public void testRouteMetric() {
        String envoyString = "{\"version\":\"1\", \"supportedAgents\":[\"TELEGRAF\"], \"labels\":{\"os\":\"LINUX\",\"arch\":\"X86_32\"},  " +
                "\"identifierName\":\"os\"}";
        client.getKVClient().put(
                EtcdUtils.fromString("/tenants/123456/envoysById/abcde"),
                EtcdUtils.fromString(envoyString)).join();

        metricRouter.route(expectedEntry, KafkaMessageType.METRIC);

        verify(kafkaEgress).send(eq("123456"), eq(KafkaMessageType.METRIC),
                eq("{\"timestamp\":\"1970-01-01T00:00:00Z\",\"accountType\":\"RCN\",\"account\":\"123456\",\"device\":\"\",\"deviceLabel\":\"\",\"deviceMetadata\":{\"os\":\"LINUX\",\"arch\":\"X86_32\"},\"monitoringSystem\":\"SALUS\",\"systemMetadata\":{\"envoyId\":\"abcde\"},\"collectionName\":\"presence_monitor\",\"collectionLabel\":\"\",\"collectionTarget\":\"123456:os:LINUX\",\"collectionMetadata\":{},\"ivalues\":{\"connected\":1},\"fvalues\":{},\"svalues\":{},\"units\":{}}"));
     }
}
