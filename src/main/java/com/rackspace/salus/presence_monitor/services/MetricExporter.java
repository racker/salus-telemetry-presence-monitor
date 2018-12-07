package com.rackspace.salus.presence_monitor.services;

import com.rackspace.salus.presence_monitor.PartitionEntry;
import com.rackspace.salus.presence_monitor.config.PresenceMonitorProperties;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

@Data
@Component
@Slf4j
public class MetricExporter extends TimerTask {
    ConcurrentHashMap<String, PartitionEntry> partitionTable;
    private MetricRouter metricRouter;
    private PresenceMonitorProperties presenceMonitorProperties;
    @Autowired
    MetricExporter(MetricRouter metricRouter, PresenceMonitorProperties presenceMonitorProperties) {
        this.metricRouter = metricRouter;
        this.presenceMonitorProperties = presenceMonitorProperties;
    }
    @Override
    public void run() {
        long startTime, elapsedTime;
        while (true) {
            log.info("Starting exporter iteration.");
            startTime = System.currentTimeMillis();
            partitionTable.entrySet().stream().forEach(partitionEntry -> {
                partitionEntry.getValue().getExistanceTable().forEach((id, existanceEntry) -> {
                    metricRouter.route(id, existanceEntry);
                });

            });
            elapsedTime = System.currentTimeMillis() - startTime;
            if (elapsedTime < (presenceMonitorProperties.getExportPeriodInSeconds() * 1000)) {
                try {
                    Thread.sleep((presenceMonitorProperties.getExportPeriodInSeconds() * 1000) - elapsedTime );
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
