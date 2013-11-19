package com.xingcloud.webinterface.monitor;

import static com.xingcloud.webinterface.utils.HttpUtils.ENABLE_SYSTEM_MONITOR;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class SystemMonitor {

  private static BlockingQueue<WIEvent> tasks = new LinkedBlockingQueue<WIEvent>();

  public static void putMonitorInfo(WIEvent mi) {
    if (!ENABLE_SYSTEM_MONITOR) {
      return;
    }
    try {
      tasks.put(mi);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public static WIEvent take() {
    if (!ENABLE_SYSTEM_MONITOR) {
      return null;
    }
    try {
      return tasks.take();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return null;
  }

}
