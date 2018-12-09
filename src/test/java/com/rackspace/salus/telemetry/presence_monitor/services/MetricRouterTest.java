/*
 *    Copyright 2018 Rackspace US, Inc.
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

import static org.mockito.Mockito.verify;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.data.ByteSequence;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.salus.telemetry.presence_monitor.types.KafkaMessageType;

import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;

import com.rackspace.salus.telemetry.presence_monitor.types.PartitionEntry;
import io.etcd.jetcd.launcher.junit.EtcdClusterResource;
import org.apache.avro.io.EncoderFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
public class MetricRouterTest {

    @Configuration
    @Import({EncoderFactory.class})
    static class TestConfig { }

    @Rule
    public final EtcdClusterResource etcd = new EtcdClusterResource("MetricRouterTest", 1);

    ObjectMapper objectMapper = new ObjectMapper();

    @MockBean
    KafkaEgress kafkaEgress;

    MetricRouter metricRouter;

    @Autowired
            EncoderFactory encoderFactory;

    Client client;

    String expectedEntryString =
            "{\"active\": true, \"resourceInfo\":{\"identifier\":\"os\",\"identifierValue\":\"LINUX\"," +
                    "\"labels\":{\"os\":\"LINUX\",\"arch\":\"X86_32\"},\"envoyId\":\"abcde\"," +
                    "\"tenantId\":\"123456\",\"address\":\"host:1234\"}}";

    String envoyString =
            "{\"version\":\"1\", \"supportedAgents\":[\"TELEGRAF\"], \"labels\":{\"os\":\"LINUX\",\"arch\":\"X86_64\"},  " +
                    "\"identifier\":\"os\"}";

    PartitionEntry.ExpectedEntry expectedEntry;
    @Before
    public void setUp() throws Exception {
      final List<String> endpoints = etcd.cluster().getClientEndpoints().stream()
              .map(URI::toString)
              .collect(Collectors.toList());
      client = com.coreos.jetcd.Client.builder().endpoints(endpoints).build();
      metricRouter = new MetricRouter(encoderFactory, kafkaEgress, client, objectMapper);
      expectedEntry = objectMapper.readValue(expectedEntryString, PartitionEntry.ExpectedEntry.class);

    }

    @Test
    public void testRouteMetric() {

        client.getKVClient().put(
                ByteSequence.fromString("/tenants/123456/envoysById/abcde"),
                ByteSequence.fromString(envoyString)).join();


        metricRouter.route(expectedEntry, KafkaMessageType.METRIC);

         verify(kafkaEgress).send("123456", KafkaMessageType.METRIC,
"{\"timestamp\":\"2018-12-09T20:55:50.564Z\",\"accountType\":\"RCN\",\"account\":\"123456\",\"device\":\"\",\"deviceLabel\":\"\",\"deviceMetadata\":{\"os\":\"LINUX\",\"arch\":\"X86_64\"},\"monitoringSystem\":\"RMII\",\"systemMetadata\":{\"envoyId\":\"abcde\"},\"collectionName\":\"presence_monitor\",\"collectionLabel\":\"\",\"collectionTarget\":\"123456:os:LINUX\",\"collectionMetadata\":{\"os\":\"LINUX\",\"arch\":\"X86_64\"},\"ivalues\":{\"connected\":1},\"fvalues\":{},\"svalues\":{},\"units\":{}}");
    }
}
