package com.xingcloud.webinterface.exception;

public class MailException extends Exception {

  /**
   *
   */
  private static final long serialVersionUID = -3926659988361938823L;

  public MailException() {
    super();
  }

  public MailException(String message, Throwable cause) {
    super(message, cause);
  }

  public MailException(String message) {
    super(message);
  }

  public MailException(Throwable cause) {
    super(cause);
  }

}
