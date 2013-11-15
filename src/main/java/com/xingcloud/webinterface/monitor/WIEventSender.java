package com.xingcloud.webinterface.monitor;

import static com.xingcloud.webinterface.utils.HttpUtils.sendMonitorInfo;

import org.apache.log4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class WIEventSender implements Runnable {
  private static final Logger LOGGER = Logger.getLogger(WIEventSender.class);

  private String id;

  private boolean enabled;

  public WIEventSender(String id, boolean enabled) {
    this.id = id;
    this.enabled = enabled;
    LOGGER.info("WebInterfaceEventSender." + id + " has been created.");
  }

  public void run() {
    while (!Thread.interrupted()) {
      try {
        WIEvent mi = SystemMonitor.take();
        if (mi == null) {
          continue;
        }

        if (enabled) {
          sendMonitorInfo(mi);
        }
//        LOGGER.info("[WIE-SENDER] - Monitor info (" + mi + ") send.");
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public String toString() {
    return "WIEventSender." + id;
  }

  public static void main(String[] args) throws InterruptedException {
    ExecutorService service = Executors.newCachedThreadPool();
    int threadCount = 10;
    for (int i = 0; i < threadCount; i++) {
      service.execute(new WIEventSender("Sender." + i, true));
    }
    service.shutdown();

    for (int i = 0; i < 20; i++) {
      SystemMonitor.putMonitorInfo(new WIEvent("event-" + i));
    }
  }
}
