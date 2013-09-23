package com.xingcloud.webinterface.model;

import com.xingcloud.webinterface.utils.WebInterfaceConstants;

public class NotAvailableNumber extends Number {
  private static final long serialVersionUID = 3493745023488432467L;

  public static final NotAvailableNumber INSTANCE = new NotAvailableNumber();

  public NotAvailableNumber() {
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
    return WebInterfaceConstants.NOT_AVAILABLE_NUMBER;
  }

}
