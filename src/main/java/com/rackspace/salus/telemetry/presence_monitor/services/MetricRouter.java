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

import com.coreos.jetcd.Client;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.salus.model.AccountType;
import com.rackspace.salus.model.ExternalMetric;
import com.rackspace.salus.model.MonitoringSystem;
import com.rackspace.salus.telemetry.model.ResourceInfo;
import com.rackspace.salus.telemetry.messaging.KafkaMessageType;
import com.rackspace.salus.telemetry.presence_monitor.types.PartitionSlice;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class MetricRouter {
    private final DateTimeFormatter universalTimestampFormatter;
    private final EncoderFactory avroEncoderFactory;
    private final KafkaEgress kafkaEgress;
    private final Client etcd;
    private final ObjectMapper objectMapper;
    private final TimestampProvider timestampProvider;
    private final Counter metricSent;

    @Autowired
    public MetricRouter(EncoderFactory avroEncoderFactory, KafkaEgress kafkaEgress, Client etcd,
        ObjectMapper objectMapper, MeterRegistry meterRegistry, TimestampProvider timestampProvider) {
        this.avroEncoderFactory = avroEncoderFactory;
        this.kafkaEgress = kafkaEgress;
        this.etcd = etcd;
        this.objectMapper = objectMapper;
        this.timestampProvider = timestampProvider;
        universalTimestampFormatter = DateTimeFormatter.ISO_INSTANT;

        metricSent = meterRegistry.counter("metricSent");
    }

    public void route(PartitionSlice.ExpectedEntry expectedEntry, KafkaMessageType type) {
        ResourceInfo resourceInfo = expectedEntry.getResourceInfo();
        String tenantId = resourceInfo.getTenantId();
        String envoyId = resourceInfo.getEnvoyId();
        String resourceKey = String.format("%s:%s", tenantId,
            resourceInfo.getResourceId());
        Map<String, String> envoyLabels = resourceInfo.getLabels();
        if (envoyLabels == null) {
            envoyLabels = Collections.emptyMap();
        }
        Map<String, String> systemMetadata;
        if (envoyId == null) {
            systemMetadata = Collections.emptyMap();
        } else {
            systemMetadata = Collections.singletonMap("envoyId", envoyId);
        }
        log.info("routing {}", resourceKey);

        Map<String, Long> iMap = new HashMap<>();
        // This is the name of the agent health metric used in v1:
        iMap.put("connected", expectedEntry.getActive() ? 1L : 0L);

        final ExternalMetric externalMetric = ExternalMetric.newBuilder()
            .setAccountType(AccountType.RCN)
            .setAccount(resourceInfo.getTenantId())
            .setTimestamp(universalTimestampFormatter.format(timestampProvider.getCurrentInstant()))
            .setDeviceMetadata(envoyLabels)
            .setCollectionMetadata(Collections.emptyMap())
            .setMonitoringSystem(MonitoringSystem.SALUS)
            .setSystemMetadata(systemMetadata)
            .setCollectionTarget(resourceKey)
            .setCollectionName("presence_monitor")
            .setFvalues(Collections.emptyMap())
            .setSvalues(Collections.emptyMap())
            .setIvalues(iMap)
            .setUnits(Collections.emptyMap())
            .build();

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            final Schema schema = externalMetric.getSchema();
            final JsonEncoder jsonEncoder = avroEncoderFactory.jsonEncoder(schema, out);

            final SpecificDatumWriter<Object> datumWriter = new SpecificDatumWriter<>(schema);
            datumWriter.write(externalMetric, jsonEncoder);
            jsonEncoder.flush();

            metricSent.increment();
            kafkaEgress.send(resourceInfo.getTenantId(), type, out.toString(StandardCharsets.UTF_8.name()));

        } catch (IOException e) {
            log.warn("Failed to Avro encode avroMetric={} original={}", externalMetric, resourceInfo, e);
            throw new RuntimeException("Failed to Avro encode metric", e);
        }
    }
}
