package com.rackspace.salus.telemetry.presence_monitor.services;

import java.time.Instant;
import org.springframework.stereotype.Component;

@Component
public class DefaultTimestampProviderImpl implements TimestampProvider {

  @Override
  public Instant getCurrentInstant() {
    return Instant.ofEpochMilli(System.currentTimeMillis());
  }
}
