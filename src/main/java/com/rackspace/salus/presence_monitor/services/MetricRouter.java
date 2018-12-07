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

package com.rackspace.salus.presence_monitor.services;

import com.coreos.jetcd.Client;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.salus.model.AccountType;
import com.rackspace.salus.model.ExternalMetric;
import com.rackspace.salus.model.MonitoringSystem;
import com.rackspace.salus.presence_monitor.PartitionEntry;
import com.rackspace.salus.presence_monitor.types.KafkaMessageType;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.rackspace.salus.services.TelemetryEdge;
import com.rackspace.salus.telemetry.etcd.types.Keys;
import com.rackspace.salus.telemetry.model.NodeInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.xml.soap.Node;

import static com.rackspace.salus.telemetry.etcd.EtcdUtils.buildKey;

@Service
@Slf4j
public class MetricRouter {
    private final DateTimeFormatter universalTimestampFormatter;
    private final EncoderFactory avroEncoderFactory;
    private final KafkaEgress kafkaEgress;
    private final Client etcd;
    private final ObjectMapper objectMapper;

    @Autowired
    public MetricRouter(EncoderFactory avroEncoderFactory, KafkaEgress kafkaEgress, Client etcd, ObjectMapper objectMapper) {
        this.avroEncoderFactory = avroEncoderFactory;
        this.kafkaEgress = kafkaEgress;
        this.etcd = etcd;
        this.objectMapper = objectMapper;
        universalTimestampFormatter = DateTimeFormatter.ISO_INSTANT;
    }

    public void route(String id, PartitionEntry.ExistanceEntry existanceEntry) {
        NodeInfo nodeInfo = existanceEntry.getNodeInfo();
        String tenantId = nodeInfo.getTenantId();
        String envoyId = nodeInfo.getEnvoyId();
        Map<String, String> envoyLabels = new HashMap<>();
        //gbj fix
        envoyLabels.put("gbj", "fix");

        if (false) {
            TelemetryEdge.EnvoySummary envoySummary = retrieveEnvoySummaryById(tenantId, envoyId).join();
            if (envoySummary == null) {
                log.warn("envoySummary not found for {}, {]", tenantId, envoyId);
                return;
            }
            envoyLabels = envoySummary.getLabelsMap();
            if (envoyLabels == null) {
                log.warn("labels not found for {}, {]", tenantId, envoyId);
            }
        }
        Map<String, Long> iMap = new HashMap<String, Long>();
        iMap.put("GBJ_GET_NAME", existanceEntry.getActive() ? 1L : 0L);

        final Instant timestamp = Instant.ofEpochMilli(System.currentTimeMillis());

        final ExternalMetric externalMetric = ExternalMetric.newBuilder()
            .setAccountType(AccountType.RCN)
            .setAccount(nodeInfo.getTenantId())
            .setTimestamp(universalTimestampFormatter.format(timestamp))
                // GBJ fix
            .setDeviceMetadata(envoyLabels)
                .setCollectionMetadata(envoyLabels)
            .setMonitoringSystem(MonitoringSystem.RMII)
            .setSystemMetadata(Collections.singletonMap("envoyId", nodeInfo.getEnvoyId()))
            .setCollectionTarget(id)
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

            kafkaEgress.send(nodeInfo.getTenantId(), KafkaMessageType.METRIC, out.toString(StandardCharsets.UTF_8.name()));

        } catch (IOException e) {
            log.warn("Failed to Avro encode avroMetric={} original={}", externalMetric, nodeInfo, e);
            throw new RuntimeException("Failed to Avro encode metric", e);
        }
    }

    private CompletableFuture<TelemetryEdge.EnvoySummary> retrieveEnvoySummaryById(String tenantId, String envoyInstanceId) {
        return etcd.getKVClient().get(
                buildKey(Keys.FMT_ENVOYS_BY_ID,
                        tenantId, envoyInstanceId))
                .thenApply(getResponse -> {
                    if (getResponse.getCount() == 0) {
                        log.warn("Unable to locate tenant={} envoyInstance={} in order to find labels",
                                tenantId, envoyInstanceId);
                        return null;
                    } else {
                        try {
                            TelemetryEdge.EnvoySummary envoySummary = objectMapper.readValue(getResponse.getKvs().get(0)
                                    .getValue().getBytes(), TelemetryEdge.EnvoySummary.class);
                            return envoySummary;
                        } catch (IOException e) {
                            log.warn("Unable to read envoy data for {}, {}", tenantId, envoyInstanceId);
                            return null;
                        }

                    }
                });
    }

}
