package com.rackspace.salus.telemetry.presence_monitor.services;

import java.time.Instant;

public interface TimestampProvider {
  Instant getCurrentInstant();
}
