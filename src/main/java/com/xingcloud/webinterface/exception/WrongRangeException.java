package com.xingcloud.webinterface.exception;

public class WrongRangeException extends Exception {

  private static final long serialVersionUID = -3196319796612172190L;

  public WrongRangeException() {
    super();
  }

  public WrongRangeException(String message, Throwable cause) {
    super(message, cause);
  }

  public WrongRangeException(String message) {
    super(message);
  }

  public WrongRangeException(Throwable cause) {
    super(cause);
  }

}
