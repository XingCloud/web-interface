package com.xingcloud.webinterface.web.listener;

import static com.xingcloud.webinterface.conf.WebInterfaceConfig.ENABLE_REDIS_CACHE;

import com.xingcloud.adhocprocessorV2.hbase.HBaseResourceManager;
import com.xingcloud.adhocprocessorV2.hbase.coprocessor.threadpool.CoprocessorTaskPool;
import com.xingcloud.adhocprocessorV2.redis.RedisResourceManager;
import com.xingcloud.basic.mail.XMailService;
import com.xingcloud.webinterface.cron.XScheduler;
import com.xingcloud.webinterface.mongo.MongoDBOperation;
import com.xingcloud.webinterface.thread.XCacheGetExecutorServiceProvider;
import com.xingcloud.webinterface.thread.XMonitorExecutorServiceProvider;
import com.xingcloud.webinterface.thread.XQueryExecutorServiceProvider;
import org.apache.log4j.Logger;
import org.quartz.SchedulerException;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.io.IOException;

public class ShutDownListener implements ServletContextListener {
  private static final Logger LOGGER = Logger.getLogger(ShutDownListener.class);

  public void contextDestroyed(ServletContextEvent arg0) {
    try {
      LOGGER.info("[SHUTDOWN-LISTENER] - HBase is shutdown.");
      HBaseResourceManager.getInstance().closeAll();
    } catch (IOException e) {
      e.printStackTrace();
    }

    MongoDBOperation.getInstance().getMongoDBManager().close();
    LOGGER.info("[SHUTDOWN-LISTENER] - MongoDB is shutdown.");

    RedisResourceManager.getInstance().destory();
    LOGGER.info("[SHUTDOWN-LISTENER] - Redis is shutdown.");

    // 查询任务占用的线程池
    LOGGER.info("[SHUTDOWN-LISTENER] - Shutdown query thread pool.");
    XQueryExecutorServiceProvider.destroy();

    // 监控的线程池
    if (ENABLE_REDIS_CACHE) {
      XMonitorExecutorServiceProvider.destory();
      LOGGER.info("[SHUTDOWN-LISTENER] - Monitor thread pool is shutdown.");
    }

    // 异步读取缓存占用的池子
    XCacheGetExecutorServiceProvider.destory();
    LOGGER.info("[SHUTDOWN-LISTENER] - Cache-getter thread pool is shutdown.");

    CoprocessorTaskPool.shutDownAllTasks();
    LOGGER.info("[SHUTDOWN-LISTENER] - Coprocessor thread pool is shutdown.");

    try {
      XScheduler.getInstance().shutdown(false);
    } catch (SchedulerException e) {
      e.printStackTrace();
    }
    LOGGER.info("[SHUTDOWN-LISTENER] - XScheduler is Shutdown.");

    XMailService.destroy();
    LOGGER.info("[SHUTDOWN-LISTENER] - XMail service is shutdown.");
  }

  public void contextInitialized(ServletContextEvent arg0) {

  }

}
