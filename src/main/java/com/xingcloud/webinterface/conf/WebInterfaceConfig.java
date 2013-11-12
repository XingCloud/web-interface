package com.xingcloud.webinterface.conf;

import com.xingcloud.xa.conf.Config;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

/**
 * User: Z J Wu Date: 13-9-5 Time: 下午12:06 Package: com.xingcloud.webinterface.conf
 */
public class WebInterfaceConfig {
  private static final Logger LOGGER = Logger.getLogger(WebInterfaceConfig.class);

  public static boolean DEBUG;
  public static boolean UI_CHECK;
  public static boolean BATCH_GROUPBY;

  public static boolean DS_LP;
  public static boolean DS_MOCK;

  public static boolean ENABLE_REDIS_CACHE;

  private static Configuration xmlConfig;

  static {
    String configFile = "/webinterface.conf.xml";
    xmlConfig = Config.createConfig(configFile, Config.ConfigFormat.xml);
    DEBUG = xmlConfig.getBoolean("common[@debug]", false);
    UI_CHECK = xmlConfig.getBoolean("common.check-ui[@enabled]", false);
    BATCH_GROUPBY = xmlConfig.getBoolean("common.batch-groupby[@enabled]", false);
    DS_LP = xmlConfig.getBoolean("data-sources.lp[@enabled]", false);
    DS_MOCK = xmlConfig.getBoolean("data-sources.mock[@enabled]", false);
    ENABLE_REDIS_CACHE = xmlConfig.getBoolean("cache.redis-cache[@enabled]", false);

    LOGGER.info("[CONFIG] - SYSTEM-DEBUG - " + DEBUG);
    LOGGER.info("[CONFIG] - UI_CHECK - " + UI_CHECK);
    LOGGER.info("[CONFIG] - DataSource(Drill) - " + DS_LP);
    LOGGER.info("[CONFIG] - DataSource(Mock) - " + DS_MOCK);
    LOGGER.info("[CONFIG] - ENABLE_REDIS_CACHE - " + ENABLE_REDIS_CACHE);
  }

  public static Configuration getConfiguration() {
    return xmlConfig;
  }

}
