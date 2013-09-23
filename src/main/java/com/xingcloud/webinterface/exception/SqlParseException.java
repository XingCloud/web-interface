package com.xingcloud.webinterface.exception;

public class SqlParseException extends Exception {

  private static final long serialVersionUID = 5627341728735554303L;

  public SqlParseException() {
    super();
  }

  public SqlParseException(String message, Throwable cause) {
    super(message, cause);
  }

  public SqlParseException(String message) {
    super(message);
  }

  public SqlParseException(Throwable cause) {
    super(cause);
  }

}
