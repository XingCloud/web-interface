package com.xingcloud.log;

import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import net.sf.json.JSONArray;
import org.apache.log4j.Logger;

/**
 * User: Z J Wu Date: 13-4-25 Time: 下午2:47 Package: com.xingcloud.webinterface.log
 */
public class ContentLogger {

  private static final Logger LOGGER = Logger.getLogger(ContentLogger.class);

  public static void logParameter(String json) {
    JSONArray ja = JSONArray.fromObject(json);
    for (Object o : ja) {
      LOGGER.info(o.toString());
    }
  }

  public static void logDescriptor(FormulaQueryDescriptor descriptor) {
    LOGGER.info(descriptor.getKey());
  }

}
