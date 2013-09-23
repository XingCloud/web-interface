package com.xingcloud.log;

import com.xingcloud.webinterface.enums.CacheState;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import org.apache.log4j.Logger;

/**
 * User: Z J Wu Date: 13-5-2 Time: 上午10:55 Package: com.xingcloud.log
 */
public class InaccurateCacheLogger {
  private static final Logger LOGGER = Logger.getLogger(InaccurateCacheLogger.class);

  public static void logCache(CacheState cacheState, FormulaQueryDescriptor descriptor) {
    if (descriptor != null) {
      LOGGER.info(cacheState.name() + " - " + descriptor.toString());
    }
  }
}
