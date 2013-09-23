package com.xingcloud.webinterface.web.servlet;

import static com.xingcloud.webinterface.enums.ErrorCode.ERR_10;
import static com.xingcloud.webinterface.enums.ErrorCode.ERR_11;
import static com.xingcloud.webinterface.enums.ErrorCode.ERR_37;

import com.google.common.base.Strings;
import com.xingcloud.webinterface.model.mongo.EventListQueryDescriptor;
import com.xingcloud.webinterface.model.result.ErrorMiscResult;
import com.xingcloud.webinterface.model.result.MiscResult;
import com.xingcloud.webinterface.model.result.SuccessfulMiscResult;
import com.xingcloud.webinterface.mongo.MongoDBOperation;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.Writer;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class GetEventListServlet extends AbstractServlet {

  private static final long serialVersionUID = -4853505713338976966L;
  private static final Logger LOGGER = Logger.getLogger(GetEventListServlet.class);

  protected void service(HttpServletRequest request, HttpServletResponse response) throws ServletException,
    IOException {
    long t1 = System.currentTimeMillis();
    long t2;
    LOGGER.info("[SERVLET-ENTER]");
    response.setCharacterEncoding("utf8");
    Writer writer = response.getWriter();
    MiscResult aor;
    String parameterJson = StringUtils.trimToNull(request.getParameter("params"));

    Map<String, Object> resultMap = null;

    if (Strings.isNullOrEmpty(parameterJson)) {
      t2 = System.currentTimeMillis();
      aor = new ErrorMiscResult(false, t2 - t1, ERR_10, "Empty params");
      writer.write(GSON.toJson(aor));
      writer.flush();
      LOGGER.info("[SERVLET-EXIT] - " + (resultMap == null ? "NULL" : "FILLED"
      ) + " in " + (t2 - t1) + " milliseconds");
      return;
    }
    LOGGER.info("Parameter Json - " + parameterJson);
    parameterJson = URLDecoder.decode(parameterJson, "utf8");
    EventListQueryDescriptor descriptor;

    try {
      descriptor = EventListQueryDescriptor.toEventListQueryDescriptor(parameterJson);
    } catch (Exception e) {
      e.printStackTrace();
      t2 = System.currentTimeMillis();
      aor = new ErrorMiscResult(false, t2 - t1, ERR_11, e + " - " + e.getMessage());
      writer.write(GSON.toJson(aor));
      writer.flush();
      LOGGER.info("[SERVLET-EXIT] - " + (resultMap == null ? "NULL" : "FILLED"
      ) + " in " + (t2 - t1) + " milliseconds");
      return;
    }

    if (Strings.isNullOrEmpty(descriptor.getTargetRow())) {
      descriptor.setTargetRow("l0");
    }

    LOGGER.info("[EVENT-DESCRIPTOR] - " + descriptor);

    try {
      Set<String> result = MongoDBOperation.getInstance().getDistinctedEventList(descriptor);
      int cnt = result == null ? 0 : result.size();
      resultMap = new HashMap<String, Object>(3);
      resultMap.put("count", cnt);
      resultMap.put("target_row", descriptor.getTargetRow());
      resultMap.put("items", result);
    } catch (Exception e) {
      e.printStackTrace();
      t2 = System.currentTimeMillis();
      aor = new ErrorMiscResult(false, t2 - t1, ERR_37, e + " - " + e.getMessage());
      writer.write(GSON.toJson(aor));
      writer.flush();
      LOGGER.info("[SERVLET-EXIT] - " + (resultMap == null ? "NULL" : "FILLED"
      ) + " in " + (t2 - t1) + " milliseconds");
      return;
    }

    t2 = System.currentTimeMillis();
    aor = new SuccessfulMiscResult(true, t2 - t1, resultMap);
    writer.write(GSON.toJson(aor));
    writer.flush();
    LOGGER.info("[SERVLET-EXIT] - " + (resultMap == null ? "NULL" : "FILLED"
    ) + " in " + (t2 - t1) + " milliseconds");
  }
}
