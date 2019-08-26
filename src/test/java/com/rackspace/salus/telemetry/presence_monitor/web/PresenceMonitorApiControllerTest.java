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

package com.rackspace.salus.telemetry.presence_monitor.web;

import static com.rackspace.salus.test.JsonTestUtils.readContent;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.rackspace.salus.telemetry.etcd.services.WorkAllocationPartitionService;
import com.rackspace.salus.telemetry.etcd.types.KeyRange;
import com.rackspace.salus.telemetry.etcd.workpart.WorkAllocator;
import com.rackspace.salus.telemetry.presence_monitor.web.controller.PresenceMonitorApiController;
import com.rackspace.salus.test.WebTestUtils;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;


@RunWith(SpringRunner.class)
@WebMvcTest(PresenceMonitorApiController.class)
public class PresenceMonitorApiControllerTest {
  @Autowired
  MockMvc mvc;

  @MockBean
  WorkAllocationPartitionService workAllocationPartitionService;

  @MockBean
  WorkAllocator workAllocator;

  @Test
  public void testGetPartitions() throws Exception {

    when(workAllocationPartitionService.getPartitions())
        .thenReturn(CompletableFuture.completedFuture(
            Arrays.asList(
                new KeyRange().setStart("0").setEnd("1"),
                new KeyRange().setStart("1").setEnd("2"),
                new KeyRange().setStart("2").setEnd("3")
            )
        ));

    final MvcResult result = mvc.perform(get(
        "/api/admin/presence-monitor/partitions")
        .contentType(MediaType.APPLICATION_JSON))
        .andExpect(request().asyncStarted())
        .andReturn();

    mvc.perform(asyncDispatch(result))
        .andDo(print())
        .andExpect(status().isOk())
        .andExpect(content()
            .contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
        .andExpect(content().json(
            readContent("PresenceMonitorApiControllerTest/testGetPartitions.json"), true));

    verify(workAllocationPartitionService).getPartitions();
    verifyNoMoreInteractions(workAllocationPartitionService);
  }

  @Test
  public void testChangePartitions() throws Exception {
    when(workAllocationPartitionService.changePartitions(anyInt()))
        .thenReturn(CompletableFuture.completedFuture(true));

    final MvcResult result = mvc.perform(put("/api/admin/presence-monitor/partitions")
        .content("{\"count\":10}")
        .contentType(MediaType.APPLICATION_JSON))
        .andExpect(request().asyncStarted())
        .andReturn();

    mvc.perform(asyncDispatch(result))
        .andDo(print())
        .andExpect(status().isOk())
        .andExpect(content().json("{\"success\": true}"));

    verify(workAllocationPartitionService).changePartitions(10);
    verifyNoMoreInteractions(workAllocationPartitionService);
  }

  @Test
  public void testChangePartitionsNoValueSet() throws Exception {
    when(workAllocationPartitionService.changePartitions(anyInt()))
        .thenReturn(CompletableFuture.completedFuture(true));

    mvc.perform(put("/api/admin/presence-monitor/partitions")
        .contentType(MediaType.APPLICATION_JSON))
        .andDo(print())
        .andExpect(status().isBadRequest());

    verifyNoMoreInteractions(workAllocationPartitionService);
  }

  @Test
  public void testChangePartitionsIllegalArgument() throws Exception {
    mvc.perform(put("/api/admin/presence-monitor/partitions")
        .content("{\"count\":-8}")
        .contentType(MediaType.APPLICATION_JSON))
        .andDo(print())
        .andExpect(status().isBadRequest())
        .andExpect(WebTestUtils.validationError("count", "must be greater than or equal to 1"));

    verifyNoMoreInteractions(workAllocationPartitionService);
  }
}
