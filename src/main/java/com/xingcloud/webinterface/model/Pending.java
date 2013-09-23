package com.xingcloud.webinterface.model;

import com.xingcloud.webinterface.utils.WebInterfaceConstants;

public class Pending {
  public static boolean isPendingPlaceholder(Object o) {
    return o instanceof Pending;
  }

  public static final Pending INSTANCE = new Pending();

  private Pending() {
  }

  @Override
  public String toString() {
    return WebInterfaceConstants.PENDING_KEY;
  }

  public static void main(String[] args) {
    System.out.println(Pending.isPendingPlaceholder(Pending.INSTANCE));
  }

}
