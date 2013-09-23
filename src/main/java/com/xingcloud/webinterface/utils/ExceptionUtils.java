package com.xingcloud.webinterface.utils;

public class ExceptionUtils {
  public static String getExceptionName(Throwable t) {
    if (t == null) {
      return null;
    }
    String name = t.getClass().getName();
    int index = name.lastIndexOf('.');
    if (index < 0) {
      return name;
    }
    return name.substring(index + 1);
  }

  public static void main(String[] args) {
    Integer i = null;

    try {
      Integer i2 = i + 1;
    } catch (Exception e) {
      String s = org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(e);
      s = s.replace("\n", "<br/>");
      System.out.println(s);
    }
  }
}
