package com.rackspace.salus.presence_monitor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.salus.common.workpart.WorkProcessor;
import com.rackspace.salus.common.workpart.config.EtcdConfig;
import com.rackspace.salus.common.workpart.config.EtcdProperties;
import com.rackspace.salus.common.workpart.config.WorkerProperties;
import com.rackspace.salus.common.workpart.services.DefaultWorkProcessor;
import com.rackspace.salus.common.workpart.services.WorkAllocator;
import com.rackspace.salus.telemetry.etcd.config.KeyHashing;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;
import com.coreos.jetcd.Client;


@Configuration
@Import({EtcdConfig.class, EtcdProperties.class, WorkerProperties.class, ObjectMapper.class, KeyHashing.class})
public class Test {
    
    @Autowired
    Test(WorkerProperties wp, Client etcd, ThreadPoolTaskScheduler taskScheduler, ObjectMapper objectMapper,
         PresenceMonitorProcessor presenceMonitorProcessor){
        this.wp = wp;
        this.etcd = etcd;
        this.taskScheduler = taskScheduler;
        this.presenceMonitorProcessor = presenceMonitorProcessor;
        System.out.println("gbj hignew3 bezin");
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
