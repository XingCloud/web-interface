package com.xingcloud.webinterface.utils;

import com.google.common.base.Strings;

import javax.servlet.http.HttpServletRequest;

/**
 * Author: mulisen Date:   12/7/12
 */
public class RequestGetter {

  private HttpServletRequest request;

  public RequestGetter(HttpServletRequest request) {
    this.request = request;
  }

  public int getInt(String parameterName, int fallback) {
    String str = request.getParameter(parameterName);
    if (!Strings.isNullOrEmpty(str)) {
      try {
        return Integer.parseInt(str);
      } catch (Exception e) {
        return fallback;
      }
    }
    return fallback;
  }
}
