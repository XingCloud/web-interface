package com.xingcloud.webinterface.monitor;

import org.apache.log4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class SystemMonitor {
  private static final Logger LOGGER = Logger.getLogger(SystemMonitor.class);

  private static BlockingQueue<MonitorInfo> tasks = new LinkedBlockingQueue<MonitorInfo>();

  public static void putMonitorInfo(MonitorInfo mi) {
    // long t1 = System.currentTimeMillis();
    try {
      tasks.put(mi);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    // long t2 = System.currentTimeMillis();
    // LOGGER.info("Put evnent into monitor use " + (t2 - t1)
    // + " milliseconds");
  }

  public static MonitorInfo take() {
    try {
      return tasks.take();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return null;
  }

  public static synchronized void list() {
    int cnt = 0;
    for (MonitorInfo mi : tasks) {
      cnt++;
      LOGGER.info("Content in monitor info tasks queue - " + cnt + ".\t" + mi);
    }
  }
}
