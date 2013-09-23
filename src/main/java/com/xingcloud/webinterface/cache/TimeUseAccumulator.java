package com.xingcloud.webinterface.cache;

import java.util.concurrent.atomic.AtomicLong;

public class TimeUseAccumulator {
  private AtomicLong redisTotalTime;

  private AtomicLong mysqltotalTime;

  public TimeUseAccumulator() {
    super();
    this.redisTotalTime = new AtomicLong(0);
    this.mysqltotalTime = new AtomicLong(0);
  }

  public long getRedisTotalTime() {
    return redisTotalTime.get();
  }

  public long getMysqltotalTime() {
    return mysqltotalTime.get();
  }

  public synchronized void updateRedisTimeUse(long timeuse) {
    long current = this.redisTotalTime.get();
    if (timeuse > current) {
      this.redisTotalTime.set(timeuse);
    }
  }

  public synchronized void updateMysqlTimeUse(long timeuse) {
    long current = this.mysqltotalTime.get();
    if (timeuse > current) {
      this.mysqltotalTime.set(timeuse);
    }
  }
}
