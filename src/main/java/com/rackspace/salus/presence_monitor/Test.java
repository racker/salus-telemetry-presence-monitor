package com.rackspace.salus.presence_monitor;

import com.rackspace.salus.common.workpart.WorkProcessor;
import com.rackspace.salus.common.workpart.config.EtcdConfig;
import com.rackspace.salus.common.workpart.config.EtcdProperties;
import com.rackspace.salus.common.workpart.config.WorkerProperties;
import com.rackspace.salus.common.workpart.services.DefaultWorkProcessor;
import com.rackspace.salus.common.workpart.services.WorkAllocator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;
import com.coreos.jetcd.Client;


@Configuration
@Import({EtcdConfig.class, EtcdProperties.class, WorkerProperties.class})
public class Test {
    @Autowired
    private WorkerProperties wp;
    @Autowired
    private Client etcd;
    @Autowired
    ThreadPoolTaskScheduler taskScheduler;
    @Bean
    public WorkAllocator getWorkAllocator() {
        return new WorkAllocator(wp, etcd, new PresenceMonitorProcessor(), taskScheduler);
    };
}
