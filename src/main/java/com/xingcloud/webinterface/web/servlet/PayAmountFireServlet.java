package com.xingcloud.webinterface.web.servlet;

import com.xingcloud.webinterface.cron.XScheduler;
import org.quartz.SchedulerException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * User: Z J Wu Date: 13-4-27 Time: 下午5:21 Package: com.xingcloud.webinterface.web.servlet
 */
public class PayAmountFireServlet extends AbstractServlet {

  @Override
  protected void service(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    String group = req.getParameter("g");
    String name = req.getParameter("n");

    try {
      XScheduler.getInstance().fireJob(group, name);
    } catch (SchedulerException e) {
      e.printStackTrace();
    }

  }
}
