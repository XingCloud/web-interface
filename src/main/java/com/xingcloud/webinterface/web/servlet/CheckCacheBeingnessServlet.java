package com.xingcloud.webinterface.web.servlet;

import static com.xingcloud.webinterface.utils.WebInterfaceConstants.DEFAULT_GSON_PLAIN;

import com.xingcloud.basic.utils.DateUtils;
import com.xingcloud.maincache.InterruptQueryException;
import com.xingcloud.maincache.MapXCache;
import com.xingcloud.maincache.XCacheException;
import com.xingcloud.maincache.XCacheOperator;
import com.xingcloud.maincache.redis.RedisXCacheOperator;
import com.xingcloud.webinterface.enums.CacheReference;
import org.apache.log4j.Logger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class CheckCacheBeingnessServlet extends AbstractServlet {

  private static final long serialVersionUID = -4853505713338976966L;
  private static final Logger LOGGER = Logger.getLogger(CheckCacheBeingnessServlet.class);

  protected void service(HttpServletRequest request, HttpServletResponse response) throws ServletException,
    IOException {
    String operation = request.getParameter("operation");
    String key = request.getParameter("key");
    PrintWriter pw = response.getWriter();
    response.setContentType("text/plain");

    Object result;
    try {
      if ("del".equals(operation)) {
        delete(key);
        result = false;
      } else if ("ref".equals(operation)) {
        result = checkBeingness(key);
      } else if ("get".equals(operation)) {
        result = DEFAULT_GSON_PLAIN.toJson(getContent(key));
      } else {
        response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Unknow operation - " + operation);
        return;
      }
      pw.write(result.toString());
      pw.flush();
    } catch (XCacheException e) {
      response
        .sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Exception while checking cache - " + e.getCause());
      e.printStackTrace();
      return;
    } catch (InterruptQueryException e) {
      result = "Interrupt-Query-placeholder";
      pw.write(result.toString());
      pw.flush();
      e.printStackTrace();
    }
  }

  private boolean checkBeingness(String key) throws XCacheException {
    XCacheOperator cacheOperator = RedisXCacheOperator.getInstance();
    return cacheOperator.exists(key);
  }

  private Object getContent(String key) throws XCacheException, InterruptQueryException {
    MapXCache xc = RedisXCacheOperator.getInstance().getMapCache(key);
    if (xc == null) {
      return null;
    }
    Map<String, Number[]> map = xc.toNumberArrayMap();
    if (map == null) {
      return null;
    }

    long timestamp = xc.getTimestamp();
    CacheReference cr;
    Map<String, Object> result = new HashMap<String, Object>();
    if (timestamp == 0) {
      cr = CacheReference.OFFLINE;
    } else {
      cr = CacheReference.ONLINE;
      String time = DateUtils.date2Long(new Date(timestamp));
      long timeElapse = Math.abs(System.currentTimeMillis() - timestamp) / 1000;
      result.put("elapse", timeElapse);
      result.put("time", time);
    }
    result.put("reference", cr);
    result.put("data", map);
    return result;
  }

  private void delete(String key) throws XCacheException {
    RedisXCacheOperator.getInstance().delete(key);
  }
}
