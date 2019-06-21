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

package com.rackspace.salus.telemetry.presence_monitor.web.controller;

import com.fasterxml.jackson.annotation.JsonView;
import com.rackspace.salus.telemetry.etcd.services.WorkAllocationPartitionService;
import com.rackspace.salus.telemetry.etcd.types.KeyRange;
import com.rackspace.salus.telemetry.etcd.types.WorkAllocationRealm;
import com.rackspace.salus.telemetry.model.View;
import com.rackspace.salus.telemetry.presence_monitor.web.model.SuccessResult;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequestMapping("/api")
public class PresenceMonitorApiController {

  private final WorkAllocationPartitionService workAllocationPartitionService;

  @Autowired
  public PresenceMonitorApiController(
      WorkAllocationPartitionService workAllocationPartitionService) {
    this.workAllocationPartitionService = workAllocationPartitionService;
  }

  @GetMapping("/admin/presence-monitor/partitions")
  @JsonView(View.Admin.class)
  public List<KeyRange> presenceMonitorPartitions() {
    return workAllocationPartitionService.getPartitions(WorkAllocationRealm.PRESENCE_MONITOR).join();
  }

  @PutMapping("/admin/presence-monitor/partitions")
  @JsonView(View.Admin.class)
  public SuccessResult changePresenceMonitorPartitions(@RequestBody int count) throws IllegalArgumentException {
    return workAllocationPartitionService.changePartitions(WorkAllocationRealm.PRESENCE_MONITOR, count)
        .thenApply(result -> new SuccessResult().setSuccess(result)).join();
  }
}