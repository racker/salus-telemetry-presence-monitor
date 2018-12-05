package com.rackspace.salus.presence_monitor;

import com.coreos.jetcd.Client;
import com.rackspace.salus.common.workpart.WorkProcessor;
import com.rackspace.salus.common.workpart.config.WorkerProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import com.rackspace.salus.common.workpart.services.WorkAllocator;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

import java.util.Arrays;


@SpringBootApplication
public class PresenceMonitorApplication implements CommandLineRunner {
//	@Autowired
//    private WorkAllocator workAllocator;
//	@Autowired
// ThreadPoolTaskScheduler taskScheduler;
//	@Autowired
//	Client etcd;

//	@Autowired
	WorkerProperties properties;
//	@Autowired
	WorkProcessor processor;
	@Autowired
	private ApplicationContext context;
	public static void main(String[] args) {
		SpringApplication.run(PresenceMonitorApplication.class, args);


PresenceMonitorApplication pm = new PresenceMonitorApplication();
		System.out.println("Let's inspect the beans provided by Spring Boot:");

		// String[] beanNames = pm.context.getBeanDefinitionNames();
		// java.util.Arrays.sort(beanNames);
		// for (String beanName : beanNames) {
		// 	System.out.println(beanName);
		// }

	System.out.println("gbj was here" );
	}
	@Autowired
	private ApplicationContext appContext;

	@Override
	public void run(String... args) throws Exception {

		String[] beans = appContext.getBeanDefinitionNames();
		Arrays.sort(beans);
		for (String bean : beans) {
//			System.out.println(bean);
		}

}
}
