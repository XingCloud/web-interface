package com.xingcloud.webinterface.model;

import com.xingcloud.webinterface.utils.WebInterfaceConstants;

public class PendingNumber extends Number {
  private static final long serialVersionUID = 3493745023488432467L;

  public static final PendingNumber INSTANCE = new PendingNumber();

  public PendingNumber() {
  }

  @Override
  public int intValue() {
    return Integer.MIN_VALUE;
  }

  @Override
  public long longValue() {
    return Long.MIN_VALUE;
  }

  @Override
  public float floatValue() {
    return Float.MIN_VALUE;
  }

  @Override
  public double doubleValue() {
    return Double.MIN_VALUE;
  }

  @Override
  public String toString() {
    return WebInterfaceConstants.PENDING_NUMBER;
  }

}
