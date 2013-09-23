package com.xingcloud.webinterface.exec;

public abstract class AbstractQueueQueryTask implements Runnable {

  protected final String WEB_SERVICE_ID = "QUERY-LP";

  public AbstractQueueQueryTask() {
    super();
  }

}
