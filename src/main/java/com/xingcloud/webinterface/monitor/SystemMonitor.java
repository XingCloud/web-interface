package com.xingcloud.webinterface.monitor;

import org.apache.log4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class SystemMonitor {
  private static final Logger LOGGER = Logger.getLogger(SystemMonitor.class);

  private static BlockingQueue<WIEvent> tasks = new LinkedBlockingQueue<WIEvent>();

  public static void putMonitorInfo(WIEvent mi) {
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

  public static WIEvent take() {
    try {
      return tasks.take();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return null;
  }

  public static synchronized void list() {
    int cnt = 0;
    for (WIEvent mi : tasks) {
      cnt++;
      LOGGER.info("Content in monitor info tasks queue - " + cnt + ".\t" + mi);
    }
  }
}
