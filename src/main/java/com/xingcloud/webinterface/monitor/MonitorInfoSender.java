package com.xingcloud.webinterface.monitor;

import static com.xingcloud.webinterface.utils.HttpUtils.sendMonitorInfo;

import org.apache.log4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MonitorInfoSender implements Runnable {
  private static final Logger LOGGER = Logger.getLogger(MonitorInfoSender.class);

  private String id;

  private boolean enabled;

  public MonitorInfoSender(String id, boolean enabled) {
    this.id = id;
    this.enabled = enabled;
    LOGGER.info("MonitorSender." + id + " has been created.");
  }

  public void run() {
    while (!Thread.interrupted()) {
      try {
        MonitorInfo mi = SystemMonitor.take();
        if (mi == null) {
          continue;
        }

        if (enabled) {
          sendMonitorInfo(mi);
        } else {
          // LOGGER.info("[MONITOR-INFO-SENDER] - Monitor info (" + mi
          // + ") sended.");
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public String toString() {
    return "MonitorInfoSender." + id;
  }

  public static void main(String[] args) throws InterruptedException {
    ExecutorService service = Executors.newCachedThreadPool();
    int threadCount = 10;
    for (int i = 0; i < threadCount; i++) {
      service.execute(new MonitorInfoSender("Sender." + i, true));
    }
    service.shutdown();

    for (int i = 0; i < 20; i++) {
      SystemMonitor.putMonitorInfo(new MonitorInfo("event-" + i));
    }
  }
}
