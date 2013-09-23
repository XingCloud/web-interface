package com.xingcloud.webinterface.thread;

import static com.xingcloud.webinterface.utils.WebInterfaceCommonUtils.THREAD_FACTORY_MAP;

import org.apache.log4j.Logger;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class XQueryExecutorServiceProvider {
  private static final Logger LOGGER = Logger.getLogger(XQueryExecutorServiceProvider.class);
  private static final UUID id = UUID.randomUUID();
  private static ExecutorService service;

  static {
    ThreadFactoryInfo tfi = THREAD_FACTORY_MAP.get("XQuery");
    synchronized (XQueryExecutorServiceProvider.class) {
      service = Executors.newFixedThreadPool(tfi.getThreadCount(), new XThreadFactory(tfi.getName()));
      LOGGER.info("[THREAD-POOL] - " + tfi.getName() + " inited.");
    }
  }

  public XQueryExecutorServiceProvider() {
    super();
  }

  public UUID getId() {
    return id;
  }

  public static ExecutorService getService() {
    return service;
  }

  public static void destroy() {
    try {
      LOGGER.info("Shutdown ExecutorService");
      service.shutdown();
      LOGGER.info("Waiting unfinished thread for 3 seconds");
      service.awaitTermination(3, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOGGER.error("counld not wait");
      // e.printStackTrace();
    } finally {
      LOGGER.info("shutdown now");
      service.shutdownNow();
    }
  }
}
