package com.xingcloud.webinterface.web.servlet;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class TestServlet extends AbstractServlet {

  private static final long serialVersionUID = 6566830784978574411L;

  protected void service(HttpServletRequest request, HttpServletResponse response) throws ServletException,
    IOException {
    System.out.println(request.getSession(true).getServletContext().getRealPath("/"));
  }

}
