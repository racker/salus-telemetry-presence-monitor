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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;
import com.rackspace.monplat.protocol.Metric;
import com.rackspace.monplat.protocol.UniversalMetricFrame;
import com.rackspace.salus.common.config.MetricTags;
import com.rackspace.salus.telemetry.messaging.KafkaMessageType;
import com.rackspace.salus.telemetry.model.MonitorType;
import com.rackspace.salus.telemetry.model.ResourceInfo;
import com.rackspace.salus.telemetry.presence_monitor.types.PartitionSlice;
import io.etcd.jetcd.Client;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.EncoderFactory;
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
    MeterRegistry meterRegistry;
    private final Counter.Builder metricSent;

    @Autowired
    public MetricRouter(EncoderFactory avroEncoderFactory, KafkaEgress kafkaEgress, Client etcd,
        ObjectMapper objectMapper, MeterRegistry meterRegistry, TimestampProvider timestampProvider) {
        this.avroEncoderFactory = avroEncoderFactory;
        this.kafkaEgress = kafkaEgress;
        this.etcd = etcd;
        this.objectMapper = objectMapper;
        this.timestampProvider = timestampProvider;
        universalTimestampFormatter = DateTimeFormatter.ISO_INSTANT;

        this.meterRegistry = meterRegistry;
        metricSent = Counter.builder("metricSent").tag(MetricTags.SERVICE_METRIC_TAG,"MetricRouter");
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
            // use LinkedHashMap to assist with testing (maintains order)
            systemMetadata = new LinkedHashMap<>();
            systemMetadata.put("envoy_id", envoyId);
            systemMetadata.put("resource_id", resourceInfo.getResourceId());
            // set supplementary values to assist with downstream metric processing
            systemMetadata.put("monitor_type", MonitorType.presence.toString());
        }
        log.info("routing {}", resourceKey);

        // This is the name of the agent health metric used in v1:
        final String measurementName = "presence_monitor";

        final UniversalMetricFrame universalMetricFrame = UniversalMetricFrame.newBuilder()
            .setAccountType(UniversalMetricFrame.AccountType.MANAGED_HOSTING)
            .setTenantId(tenantId)
            .putAllDeviceMetadata(envoyLabels)
            .putAllSystemMetadata(systemMetadata)
            .setMonitoringSystem(UniversalMetricFrame.MonitoringSystem.SALUS)
            .addMetrics(Metric.newBuilder()
                .setGroup(measurementName)
                .setTimestamp(getProtoBufTimestamp(timestampProvider.getCurrentInstant()))
                .setName("connected")
                .setInt(expectedEntry.getActive() ? 1L : 0L)
                .putAllMetadata(Collections.emptyMap())
                .build())
            .setDevice(resourceKey)
            .build();

        try {
            metricSent
                .tags(MetricTags.OPERATION_METRIC_TAG, "route", MetricTags.OBJECT_TYPE_METRIC_TAG,
                    "metric").register(meterRegistry).increment();
            kafkaEgress.send(resourceInfo.getTenantId(), type,
                JsonFormat.printer().print(universalMetricFrame));
        } catch (IOException e) {
            log.warn("Failed to encode metricFrame={} from={}", universalMetricFrame,
                resourceInfo, e);
            throw new RuntimeException("Failed to encode metric", e);
        }
    }

    private Timestamp getProtoBufTimestamp(Instant timestamp) {
        return Timestamp.newBuilder().setSeconds(timestamp.getEpochSecond())
            .setNanos(timestamp.getNano()).build();
    }
}
