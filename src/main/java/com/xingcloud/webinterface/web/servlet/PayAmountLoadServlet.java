package com.xingcloud.webinterface.web.servlet;

import static com.xingcloud.basic.utils.DateUtils.date2Short;
import static com.xingcloud.basic.utils.DateUtils.yesterday;
import static com.xingcloud.webinterface.cron.AllProjectPayAmountJob.PAY_AMOUNT_DATA_OUTPUT;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.DEFAULT_GSON_PLAIN;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.GENERIC_SEPARATOR;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

/**
 * User: Z J Wu Date: 13-4-27 Time: 下午5:21 Package: com.xingcloud.webinterface.web.servlet
 */
public class PayAmountLoadServlet extends AbstractServlet {

  @Override
  protected void service(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    String operateDate = date2Short(yesterday());
    File f = new File(PAY_AMOUNT_DATA_OUTPUT + operateDate);

    BufferedReader br = null;
    String line;
    String[] arr;
    List<String> l = new ArrayList<String>();
    try {
      br = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
      while ((line = br.readLine()) != null) {
        try {
          arr = line.split(String.valueOf(GENERIC_SEPARATOR));
          l.add(arr[0]);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (br != null) {
        br.close();
      }
    }

    Writer w = resp.getWriter();
    w.write(DEFAULT_GSON_PLAIN.toJson(l));
    w.flush();
  }
}
