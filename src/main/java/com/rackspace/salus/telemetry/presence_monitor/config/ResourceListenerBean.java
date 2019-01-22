/*
 *    Copyright 2019 Rackspace US, Inc.
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

package com.rackspace.salus.telemetry.presence_monitor.config;


import com.rackspace.salus.telemetry.presence_monitor.services.ResourceListener;
import com.rackspace.salus.telemetry.presence_monitor.types.PartitionSlice;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ConcurrentHashMap;


@Configuration
public class ResourceListenerBean {
    private static ConcurrentHashMap<String, PartitionSlice> partitionTable = new ConcurrentHashMap<>();

    @Bean
    public ConcurrentHashMap<String, PartitionSlice> getPartitionTable() {
        return partitionTable;
    }

    @Bean("resourceListener")
    public ResourceListener getResourceListener() {
        return new ResourceListener(partitionTable);
    }
}
