package com.rackspace.salus.presence_monitor;

import lombok.extern.slf4j.Slf4j;
import com.rackspace.salus.common.workpart.WorkProcessor;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class PresenceMonitorProcessor implements WorkProcessor {

  @Override
  public void start(String id, String content) {
    log.info("GBJ Starting work on id={}, content={}", id, content);
  }

  @Override
  public void update(String id, String content) {
    log.info("GBJ Updating work on id={}, content={}", id, content);
  }

  @Override
  public void stop(String id, String content) {
    log.info("GBJ Stopping work on id={}, content={}", id, content);
  }
}
