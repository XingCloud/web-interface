package com.xingcloud.webinterface.exception;

/**
 * 同步指标异常
 *
 * @author Z J Wu
 */
public class MetricSyncException extends Exception {

  private static final long serialVersionUID = 8671498566180293608L;

  public MetricSyncException() {
    super();
  }

  public MetricSyncException(String message, Throwable cause) {
    super(message, cause);
  }

  public MetricSyncException(String message) {
    super(message);
  }

  public MetricSyncException(Throwable cause) {
    super(cause);
  }

}
