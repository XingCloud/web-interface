package com.xingcloud.webinterface.exception;

/**
 * Mongo异常
 *
 * @author Z J Wu
 */
public class MongodbException extends Exception {

  private static final long serialVersionUID = 6299267329499244280L;

  public MongodbException() {
    super();
  }

  public MongodbException(String message, Throwable cause) {
    super(message, cause);
  }

  public MongodbException(String message) {
    super(message);
  }

  public MongodbException(Throwable cause) {
    super(cause);
  }

}
