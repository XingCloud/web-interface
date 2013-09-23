package com.xingcloud.webinterface.web.servlet;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;

public class AbstractServlet extends HttpServlet {
  private static final long serialVersionUID = -6989226763344049258L;
  protected static final String LOG_ID = "[SERVLET]\t";
  protected static Gson GSON = new GsonBuilder().serializeNulls().excludeFieldsWithoutExposeAnnotation().create();
  protected String contentType;

  public void init(ServletConfig config) throws ServletException {
    contentType = config.getInitParameter("CONTENT_TYPE");
  }

}
