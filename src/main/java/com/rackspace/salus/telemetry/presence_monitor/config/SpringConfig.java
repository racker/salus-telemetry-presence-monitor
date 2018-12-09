package com.rackspace.salus.telemetry.presence_monitor.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.salus.common.workpart.config.EtcdConfig;
import com.rackspace.salus.common.workpart.config.EtcdProperties;
import com.rackspace.salus.common.workpart.config.WorkerProperties;
import com.rackspace.salus.common.workpart.services.WorkAllocator;
import com.rackspace.salus.telemetry.presence_monitor.services.PresenceMonitorProcessor;
import com.rackspace.salus.telemetry.etcd.config.KeyHashing;
import com.rackspace.salus.telemetry.etcd.services.EnvoyResourceManagement;
import org.apache.avro.io.EncoderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import com.coreos.jetcd.Client;


@Configuration
@Import({EtcdConfig.class, EtcdProperties.class, WorkerProperties.class, ObjectMapper.class,
        KeyHashing.class, EnvoyResourceManagement.class, EncoderFactory.class})
public class SpringConfig {
    
    @Autowired
    SpringConfig(WorkerProperties wp, Client etcd, ThreadPoolTaskScheduler taskScheduler, ObjectMapper objectMapper,
                 PresenceMonitorProcessor presenceMonitorProcessor){
        this.wp = wp;
        this.etcd = etcd;
        this.taskScheduler = taskScheduler;
        this.presenceMonitorProcessor = presenceMonitorProcessor;
    }
    private WorkerProperties wp;
    private Client etcd;
    private ThreadPoolTaskScheduler taskScheduler;
    private ObjectMapper objectMapper;
    private PresenceMonitorProcessor presenceMonitorProcessor;
    @Bean
    public WorkAllocator getWorkAllocator() {
        return new WorkAllocator(wp, etcd, presenceMonitorProcessor, taskScheduler);
    };
}
