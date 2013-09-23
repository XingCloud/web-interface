package com.xingcloud.webinterface.exception;

public class NecessaryCollectionEmptyException extends Exception {

  private static final long serialVersionUID = 1491194081185395167L;

  public NecessaryCollectionEmptyException() {
    super();
  }

  public NecessaryCollectionEmptyException(String message, Throwable cause) {
    super(message, cause);
  }

  public NecessaryCollectionEmptyException(String message) {
    super(message);
  }

  public NecessaryCollectionEmptyException(Throwable cause) {
    super(cause);
  }

}
