package com.xingcloud.webinterface.web.servlet;

import static com.xingcloud.basic.Constants.SEPARATOR_CHAR_EVENT;
import static com.xingcloud.basic.mail.XMail.sendNewWebExceptionMail;
import static com.xingcloud.webinterface.enums.ErrorCode.ERR_1;
import static com.xingcloud.webinterface.enums.ErrorCode.ERR_11;
import static com.xingcloud.webinterface.enums.ErrorCode.ERR_12;
import static com.xingcloud.webinterface.enums.ErrorCode.ERR_1275;
import static com.xingcloud.webinterface.enums.ErrorCode.ERR_12751;
import static com.xingcloud.webinterface.enums.ErrorCode.ERR_20;
import static com.xingcloud.webinterface.enums.ErrorCode.ERR_22;
import static com.xingcloud.webinterface.enums.ErrorCode.ERR_33;
import static com.xingcloud.webinterface.enums.ErrorCode.ERR_34;
import static com.xingcloud.webinterface.enums.ErrorCode.ERR_35;
import static com.xingcloud.webinterface.enums.ErrorCode.ERR_36;
import static com.xingcloud.webinterface.enums.ErrorCode.ERR_37;
import static com.xingcloud.webinterface.enums.ErrorCode.ERR_39;
import static com.xingcloud.webinterface.enums.QueryType.COMMON;
import static com.xingcloud.webinterface.model.formula.FormulaParameterContainer.json2Containers;
import static com.xingcloud.webinterface.monitor.MonitorInfo.MI_EXCEPTION_DATA_FILL_EXCEPTION;
import static com.xingcloud.webinterface.monitor.MonitorInfo.MI_EXCEPTION_INTERRUPT_QUERY_EXCEPTION;
import static com.xingcloud.webinterface.monitor.MonitorInfo.MI_EXCEPTION_NUMBER_OF_DAY_EXCEPTION;
import static com.xingcloud.webinterface.monitor.MonitorInfo.MI_EXCEPTION_PARSE_EXCEPTION;
import static com.xingcloud.webinterface.monitor.MonitorInfo.MI_EXCEPTION_PARSE_INCREMENTAL_EXCEPTION;
import static com.xingcloud.webinterface.monitor.MonitorInfo.MI_EXCEPTION_PARSE_JSON_EXCEPTION;
import static com.xingcloud.webinterface.monitor.MonitorInfo.MI_EXCEPTION_PREFIX;
import static com.xingcloud.webinterface.monitor.MonitorInfo.MI_EXCEPTION_RANGING_EXCEPTION;
import static com.xingcloud.webinterface.monitor.MonitorInfo.MI_EXCEPTION_SEGMENT_EXCEPTION;
import static com.xingcloud.webinterface.monitor.MonitorInfo.MI_EXCEPTION_UI_CHECK_EXCEPTION;
import static com.xingcloud.webinterface.monitor.MonitorInfo.MI_EXCEPTION_XPARAMETER_EXCEPTION;
import static com.xingcloud.webinterface.monitor.MonitorInfo.MI_EXCEPTION_XQUERY_EXCEPTION;
import static com.xingcloud.webinterface.monitor.MonitorInfo.MI_QUERY_ENTER;
import static com.xingcloud.webinterface.monitor.MonitorInfo.MI_STR_TIME_USE_WHOLE_QUERY;
import static com.xingcloud.webinterface.monitor.SystemMonitor.putMonitorInfo;
import static com.xingcloud.webinterface.utils.ExceptionUtils.getExceptionName;

import com.google.common.base.Strings;
import com.xingcloud.maincache.InterruptQueryException;
import com.xingcloud.webinterface.enums.ErrorCode;
import com.xingcloud.webinterface.enums.Order;
import com.xingcloud.webinterface.enums.QueryType;
import com.xingcloud.webinterface.exception.DataFillingException;
import com.xingcloud.webinterface.exception.NecessaryCollectionEmptyException;
import com.xingcloud.webinterface.exception.NumberOfDayException;
import com.xingcloud.webinterface.exception.ParseIncrementalException;
import com.xingcloud.webinterface.exception.ParseJsonException;
import com.xingcloud.webinterface.exception.RangingException;
import com.xingcloud.webinterface.exception.SegmentException;
import com.xingcloud.webinterface.exception.UICheckException;
import com.xingcloud.webinterface.exception.UICheckTimeoutException;
import com.xingcloud.webinterface.exception.XParameterException;
import com.xingcloud.webinterface.exception.XQueryException;
import com.xingcloud.webinterface.exec.CommonQueryExecutor;
import com.xingcloud.webinterface.exec.GroupByQueryExecutor;
import com.xingcloud.webinterface.exec.QueryExecutor;
import com.xingcloud.webinterface.model.formula.FormulaParameterContainer;
import com.xingcloud.webinterface.model.result.EmptyQueryResult;
import com.xingcloud.webinterface.model.result.ErrorQueryResult;
import com.xingcloud.webinterface.model.result.QueryResult;
import com.xingcloud.webinterface.monitor.MonitorInfo;
import com.xingcloud.webinterface.utils.RequestGetter;
import com.xingcloud.webinterface.utils.WebInterfaceConstants;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.Writer;
import java.net.URLDecoder;
import java.text.ParseException;
import java.util.List;

public class QueryServlet extends AbstractServlet {

  private static final long serialVersionUID = 5708108800795336373L;

  private static final Logger LOGGER = Logger.getLogger(QueryServlet.class);

  protected void service(HttpServletRequest request, HttpServletResponse response) throws ServletException,
    IOException {
    LOGGER.info("[SERVLET] - Enter(" + request.getRemoteUser() + ")");
    putMonitorInfo(MI_QUERY_ENTER);
    long t1 = System.currentTimeMillis();
    response.setCharacterEncoding("utf-8");
    response.setContentType(contentType);
    Writer writer = response.getWriter();

    QueryResult rqr = null;
    List<FormulaParameterContainer> containers;
    String parameterJson = null;
    long before = System.currentTimeMillis();
    long after, milli;

    RequestGetter getter = new RequestGetter(request);

    ErrorCode expectedError = ERR_1;
    try {
      parameterJson = request.getParameter("params");
//      LOGGER.info(
//        "[SERVLET] - Parameter got - " + (StringUtils.isBlank(parameterJson)
//                                          ? "NULL" : "NORMAL"
//        ));
      if (Strings.isNullOrEmpty(parameterJson)) {
        expectedError = ERR_11;
        throw new XParameterException("Empty params");
      }
      parameterJson = URLDecoder.decode(parameterJson, "utf8");
//      logParameter(parameterJson);
      containers = json2Containers(parameterJson);

      expectedError = ERR_1275;
      // 验证containers中的每个container
      for (FormulaParameterContainer container : containers) {
        container.validate();
      }

      if (CollectionUtils.isEmpty(containers)) {
        expectedError = ERR_1275;
        throw new XParameterException("There is no any valid container.");
      }

      QueryType qt = containers.get(0).getQueryType();
      expectedError = ERR_12;
      if (COMMON.equals(qt)) {
        rqr = queryCommon(containers);
      } else {
        int index = getter.getInt("index", 0);
        int pageSize = getter.getInt("pagesize", WebInterfaceConstants.PAGE_SIZE);
        String filterString = StringUtils.trimToNull(request.getParameter("filter"));
        String orderBy = StringUtils.trimToNull(request.getParameter("orderby"));
        String orderString = StringUtils.trimToNull(request.getParameter("order"));
        Order order;

        if (Order.ASC.name().equals(orderString)) {
          order = Order.ASC;
        } else {
          order = Order.DESC;
        }

        rqr = queryGroupBy(containers, filterString, orderBy, order, index, pageSize);
      }

      if (rqr == null) {
        rqr = EmptyQueryResult.INSTANCE;
      }
    } catch (XQueryException e) {
      rqr = new ErrorQueryResult(ERR_37.name(), e.getMessage());
      putMonitorInfo(MI_EXCEPTION_XQUERY_EXCEPTION);
      sendMail(request, e, parameterJson);
      e.printStackTrace();
    } catch (XParameterException e) {
      e.printStackTrace();
      rqr = new ErrorQueryResult(expectedError.name(), e.getMessage());
      putMonitorInfo(MI_EXCEPTION_XPARAMETER_EXCEPTION);
    } catch (SegmentException e) {
      e.printStackTrace();
      rqr = new ErrorQueryResult(ERR_22.name(), e.getMessage());
      putMonitorInfo(MI_EXCEPTION_SEGMENT_EXCEPTION);
    } catch (ParseException e) {
      e.printStackTrace();
      rqr = new ErrorQueryResult(ERR_20.name(), e.getMessage());
      putMonitorInfo(MI_EXCEPTION_PARSE_EXCEPTION);
    } catch (ParseJsonException e) {
      e.printStackTrace();
      rqr = new ErrorQueryResult(ERR_12.name(), e.getMessage());
      putMonitorInfo(MI_EXCEPTION_PARSE_JSON_EXCEPTION);
    } catch (InterruptQueryException e) {
      e.printStackTrace();
      rqr = new ErrorQueryResult(ERR_33.name(), e.getMessage());
      putMonitorInfo(MI_EXCEPTION_INTERRUPT_QUERY_EXCEPTION);
    } catch (ParseIncrementalException e) {
      e.printStackTrace();
      rqr = new ErrorQueryResult(ERR_34.name(), e.getMessage());
      putMonitorInfo(MI_EXCEPTION_PARSE_INCREMENTAL_EXCEPTION);
    } catch (NumberOfDayException e) {
      e.printStackTrace();
      rqr = new ErrorQueryResult(ERR_12751.name(), e.getMessage());
      putMonitorInfo(MI_EXCEPTION_NUMBER_OF_DAY_EXCEPTION);
    } catch (DataFillingException e) {
      e.printStackTrace();
      rqr = new ErrorQueryResult(ERR_39.name(), e.getMessage());
      putMonitorInfo(MI_EXCEPTION_DATA_FILL_EXCEPTION);
    } catch (RangingException e) {
      e.printStackTrace();
      rqr = new ErrorQueryResult(ERR_35.name(), e.getMessage());
      putMonitorInfo(MI_EXCEPTION_RANGING_EXCEPTION);
    } catch (UICheckException e) {
      e.printStackTrace();
      rqr = new ErrorQueryResult(ERR_36.name(), e.getMessage());
      putMonitorInfo(MI_EXCEPTION_UI_CHECK_EXCEPTION);
    } catch (Exception e) {
      rqr = new ErrorQueryResult(ERR_1.name(), e.toString() + " - " + e.getMessage());
      putMonitorInfo(new MonitorInfo(MI_EXCEPTION_PREFIX + SEPARATOR_CHAR_EVENT + getExceptionName(e)));
      sendMail(request, e, parameterJson);
      e.printStackTrace();
    } finally {
      after = System.currentTimeMillis();
      milli = after - before;
      if (rqr != null) {
        rqr.setMilli(milli);
      }
    }

    String json = GSON.toJson(rqr);
    writer.write(json);
    writer.flush();
    long t2 = System.currentTimeMillis();

    putMonitorInfo(new MonitorInfo(MI_STR_TIME_USE_WHOLE_QUERY, t2 - t1));
    LOGGER.info("[SERVLET] - Servlet finished in " + (t2 - t1) + " milliseconds");
  }

  private QueryResult queryCommon(List<FormulaParameterContainer> containers) throws XQueryException, SegmentException,
    XParameterException, ParseException, UICheckException, DataFillingException, RangingException,
    ParseIncrementalException, NecessaryCollectionEmptyException, InterruptQueryException, UICheckTimeoutException {
    QueryExecutor executor = new CommonQueryExecutor(containers);
    return executor.getResult();
  }

  private QueryResult queryGroupBy(List<FormulaParameterContainer> containers, String filterString, String orderBy,
                                   Order order, int index, int pageSize) throws XQueryException, XParameterException,
    SegmentException, ParseException, UICheckException, DataFillingException, NecessaryCollectionEmptyException,
    RangingException, ParseIncrementalException, InterruptQueryException, UICheckTimeoutException {
    QueryExecutor executor = new GroupByQueryExecutor(containers, filterString, orderBy, order, index, pageSize);
    return executor.getResult();
  }

  private void sendMail(HttpServletRequest request, Exception e, String parameterJson) {
    String remoteUser = request.getRemoteUser();
    String remoteAddr = request.getRemoteAddr();
    String remoteHost = request.getRemoteHost();
    sendNewWebExceptionMail(e, parameterJson, remoteUser, remoteAddr, remoteHost);
  }

}
