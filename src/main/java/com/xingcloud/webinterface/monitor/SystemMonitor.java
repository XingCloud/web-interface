package com.xingcloud.webinterface.monitor;

import static com.xingcloud.webinterface.utils.HttpUtils.ENABLE_SYSTEM_MONITOR;

import org.apache.log4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class SystemMonitor {
  private static final Logger LOGGER = Logger.getLogger(SystemMonitor.class);

  private static BlockingQueue<MonitorInfo> tasks = new LinkedBlockingQueue<MonitorInfo>();

  public static void putMonitorInfo(MonitorInfo mi) {
    if (ENABLE_SYSTEM_MONITOR) {
      try {
        tasks.put(mi);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public static MonitorInfo take() {
    if (ENABLE_SYSTEM_MONITOR) {
      try {
        return tasks.take();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    return null;
  }

}
