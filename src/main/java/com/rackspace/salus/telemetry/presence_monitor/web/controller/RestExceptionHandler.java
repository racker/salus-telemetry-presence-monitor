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

package com.rackspace.salus.telemetry.presence_monitor.web.controller;

import com.rackspace.salus.common.config.MetricNames;
import com.rackspace.salus.common.config.MetricTags;
import com.rackspace.salus.common.errors.ResponseMessages;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import javax.servlet.http.HttpServletRequest;
import org.hibernate.JDBCException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.servlet.error.ErrorAttributes;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.HandlerMapping;

@ControllerAdvice(basePackages = "com.rackspace.salus.telemetry.presence_monitor.web")
@ResponseBody
public class RestExceptionHandler extends
    com.rackspace.salus.common.web.AbstractRestExceptionHandler {

  MeterRegistry meterRegistry;
  private final Counter.Builder presenceMonitorErrorCounter;

  @Autowired
  public RestExceptionHandler(ErrorAttributes errorAttributes, MeterRegistry meterRegistry) {
    super(errorAttributes);
    this.meterRegistry = meterRegistry;
    presenceMonitorErrorCounter = Counter.builder(MetricNames.SERVICE_OPERATION_FAILED);
  }

  @ExceptionHandler({JDBCException.class})
  public ResponseEntity<?> handleJDBCException(
      HttpServletRequest request, Exception e) {
    presenceMonitorErrorCounter
        .tags(MetricTags.URI_METRIC_TAG,request.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE).toString(),MetricTags.EXCEPTION_METRIC_TAG,e.getClass().getSimpleName())
        .register(meterRegistry).increment();
    if (e instanceof DataIntegrityViolationException) {
      return respondWith(request, HttpStatus.BAD_REQUEST, e.getMessage());
    } else {
      return respondWith(request, HttpStatus.SERVICE_UNAVAILABLE, ResponseMessages.jdbcExceptionMessage);
    }
  }
}
