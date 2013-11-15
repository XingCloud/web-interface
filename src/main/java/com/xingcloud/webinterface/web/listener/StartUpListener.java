package com.xingcloud.webinterface.web.listener;

import static com.xingcloud.webinterface.utils.HttpUtils.ENABLE_SYSTEM_MONITOR;

import com.xingcloud.webinterface.cache.UITableChecker;
import com.xingcloud.webinterface.conf.WebInterfaceConfig;
import com.xingcloud.webinterface.monitor.WIEventSender;
import com.xingcloud.webinterface.remote.WebServiceProvider;
import com.xingcloud.webinterface.thread.XMonitorExecutorServiceProvider;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.util.concurrent.ExecutorService;

public class StartUpListener implements ServletContextListener {
  private static final Logger LOGGER = Logger.getLogger(StartUpListener.class);

  public void contextDestroyed(ServletContextEvent arg0) {
  }

  public void contextInitialized(ServletContextEvent arg0) {

    WebServiceProvider.init();

    Configuration configuration = WebInterfaceConfig.getConfiguration();
    if (ENABLE_SYSTEM_MONITOR) {
      ExecutorService service = XMonitorExecutorServiceProvider.getService();
      for (int i = 0; i < configuration.getInt("system-monitor[@senders]"); i++) {
        service.execute(new WIEventSender(new Integer(1 + i).toString(), ENABLE_SYSTEM_MONITOR));
      }
      service.shutdown();
    } else {
      LOGGER.info("[MONITOR] - Disabled.");
    }

    UITableChecker.loadInitedUITables();

//    try {
//      XScheduler.getInstance().start();
//    } catch (SchedulerException e) {
//      e.printStackTrace();
//    }

  }

}
