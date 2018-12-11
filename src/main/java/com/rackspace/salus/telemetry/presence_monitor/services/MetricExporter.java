package com.rackspace.salus.telemetry.presence_monitor.services;

import com.rackspace.salus.telemetry.presence_monitor.config.PresenceMonitorProperties;
import com.rackspace.salus.telemetry.presence_monitor.types.KafkaMessageType;
import com.rackspace.salus.telemetry.presence_monitor.types.PartitionSlice;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Data
@Component
@Slf4j
public class MetricExporter extends TimerTask {

    ConcurrentHashMap<String, PartitionSlice> partitionTable;
    private MetricRouter metricRouter;
    private PresenceMonitorProperties presenceMonitorProperties;
    private final Timer metricExporterDuration;

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
        while (true) {
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
}
