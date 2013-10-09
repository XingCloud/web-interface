package com.xingcloud.webinterface.exec;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

public abstract class AbstractQueueQueryTask implements Runnable {
  private static final Logger LOGGER = Logger.getLogger(AbstractQueueQueryTask.class);

  protected final String WEB_SERVICE_ID = "QUERY-LP";

  public AbstractQueueQueryTask() {
    super();
  }

  protected void writeLPString2LocalLog(String planString) {
    if (StringUtils.isBlank(planString)) {
      return;
    }
    planString = planString.replace("\n", "");
    LOGGER.info(planString);
  }

}
