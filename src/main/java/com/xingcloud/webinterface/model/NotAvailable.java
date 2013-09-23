package com.xingcloud.webinterface.model;

import com.xingcloud.webinterface.utils.WebInterfaceConstants;

public class NotAvailable {
  public static boolean isNotAvailablePlaceholder(Object o) {
    return o instanceof NotAvailable;
  }

  public static final NotAvailable INSTANCE = new NotAvailable();

  private NotAvailable() {
  }

  @Override
  public String toString() {
    return WebInterfaceConstants.NOT_AVAILABLE_KEY;
  }

}
