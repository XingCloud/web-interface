package com.xingcloud.webinterface.exception;

public class UnknownOperatorException extends Exception {

  private static final long serialVersionUID = -1449015043140309251L;

  public UnknownOperatorException() {
    super();
  }

  public UnknownOperatorException(String message, Throwable cause) {
    super(message, cause);
  }

  public UnknownOperatorException(String message) {
    super(message);
  }

  public UnknownOperatorException(Throwable cause) {
    super(cause);
  }

}
