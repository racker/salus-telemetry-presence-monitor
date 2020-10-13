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

import com.rackspace.salus.telemetry.messaging.KafkaMessageType;
import com.rackspace.salus.telemetry.presence_monitor.config.PresenceMonitorProperties;
import com.rackspace.salus.telemetry.presence_monitor.types.PartitionSlice;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@EqualsAndHashCode(callSuper = true)
@Data
@Component
@Slf4j
public class MetricExporter extends TimerTask implements AutoCloseable {

    ConcurrentHashMap<String, PartitionSlice> partitionTable;
    private MetricRouter metricRouter;
    private PresenceMonitorProperties presenceMonitorProperties;
    private final Timer metricExporterDuration;
    private boolean stopped;

    @Autowired
    MetricExporter(MetricRouter metricRouter, PresenceMonitorProperties presenceMonitorProperties,
        MeterRegistry meterRegistry) {
        this.metricRouter = metricRouter;
        this.presenceMonitorProperties = presenceMonitorProperties;

        metricExporterDuration = meterRegistry.timer("metricExporterDuration");
    }

    @Override
    public void run() {
        long startTime, elapsedTime;
        while (!stopped) {
            log.info("Starting exporter iteration.");
            startTime = System.currentTimeMillis();
            partitionTable.entrySet().forEach(partitionSlice -> {
                partitionSlice.getValue().getExpectedTable().forEach((id, expectedEntry) -> {
                    metricRouter.route(expectedEntry, KafkaMessageType.METRIC);
                });

            });
            elapsedTime = System.currentTimeMillis() - startTime;
            metricExporterDuration.record(elapsedTime, TimeUnit.MILLISECONDS);

            if (elapsedTime < (presenceMonitorProperties.getExportPeriod().toMillis())) {
                try {
                    Thread.sleep((presenceMonitorProperties.getExportPeriod().toMillis()) - elapsedTime );
                } catch (InterruptedException e) {
                    //exit loop when interrupted
                    break;
                }
            } else {
                log.warn("Metrics exporter unable to finish in time.");
            }
        }
    }

    @Override
    public void close() throws Exception {
        stopped = true;
    }
}
