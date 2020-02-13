/*
 * Copyright 2020 Rackspace US, Inc.
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

package com.rackspace.salus.telemetry.presence_monitor;

import com.rackspace.salus.common.config.AutoConfigureSalusAppMetrics;
import com.rackspace.salus.common.messaging.EnableSalusKafkaMessaging;
import com.rackspace.salus.common.util.DumpConfigProperties;
import com.rackspace.salus.common.web.EnableRoleBasedJsonViews;
import com.rackspace.salus.telemetry.etcd.EnableEtcd;
import com.rackspace.salus.telemetry.presence_monitor.types.PartitionSlice;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableSalusKafkaMessaging
@EnableScheduling
@EnableEtcd
@EnableRoleBasedJsonViews
@AutoConfigureSalusAppMetrics
public class PresenceMonitorApplication {

	@Bean
	public ConcurrentHashMap<String, PartitionSlice> partitionTable() {
		return new ConcurrentHashMap<>();
	}

	public static void main(String[] args) {
		DumpConfigProperties.process(args);

		SpringApplication.run(PresenceMonitorApplication.class, args);
	}
}
