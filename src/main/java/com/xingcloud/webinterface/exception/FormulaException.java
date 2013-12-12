package com.xingcloud.webinterface.exception;

/**
 * User: Z J Wu Date: 13-12-11 Time: 下午5:02 Package: com.xingcloud.webinterface.exception
 */
public class FormulaException extends Exception {
  public FormulaException() {
  }

  public FormulaException(String message) {
    super(message);
  }

  public FormulaException(String message, Throwable cause) {
    super(message, cause);
  }

  public FormulaException(Throwable cause) {
    super(cause);
  }
}
