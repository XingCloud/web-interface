package com.xingcloud.webinterface.exec;

import static com.xingcloud.webinterface.monitor.SystemMonitor.putMonitorInfo;
import static com.xingcloud.webinterface.monitor.WIEvent.WIE_STR_TIMEUSE_CALCULATE;

import com.xingcloud.maincache.InterruptQueryException;
import com.xingcloud.webinterface.enums.GroupByType;
import com.xingcloud.webinterface.enums.Order;
import com.xingcloud.webinterface.exception.DataFillingException;
import com.xingcloud.webinterface.exception.FormulaException;
import com.xingcloud.webinterface.exception.NecessaryCollectionEmptyException;
import com.xingcloud.webinterface.exception.ParseIncrementalException;
import com.xingcloud.webinterface.exception.RangingException;
import com.xingcloud.webinterface.exception.SegmentException;
import com.xingcloud.webinterface.exception.UICheckException;
import com.xingcloud.webinterface.exception.UICheckTimeoutException;
import com.xingcloud.webinterface.exception.XParameterException;
import com.xingcloud.webinterface.exception.XQueryException;
import com.xingcloud.webinterface.model.formula.FormulaParameterContainer;
import com.xingcloud.webinterface.model.formula.FormulaParameterItem;
import com.xingcloud.webinterface.model.formula.GroupByFormulaParameterItem;
import com.xingcloud.webinterface.model.intermediate.GroupByIdResult;
import com.xingcloud.webinterface.model.result.GroupByQueryResult;
import com.xingcloud.webinterface.model.result.QueryResult;
import com.xingcloud.webinterface.monitor.WIEvent;
import org.apache.commons.collections.CollectionUtils;
import org.apache.log4j.Logger;

import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GroupByQueryExecutor extends AbstractQueryExecutor {
  private static final Logger LOGGER = Logger.getLogger(GroupByQueryExecutor.class);

  private List<FormulaParameterContainer> containers;

  private String filterString;

  private String orderBy;

  private Order order;

  private int index;

  private int pageSize;

  public GroupByQueryExecutor(List<FormulaParameterContainer> containers, String filterString, String orderBy,
                              Order order, int index, int pageSize) {
    this.containers = containers;
    this.containers = containers;
    this.filterString = filterString;
    this.orderBy = orderBy;
    this.order = order;
    this.index = index;
    this.pageSize = pageSize;
  }

  public QueryResult getResult() throws XQueryException, SegmentException, XParameterException, ParseException,
    UICheckException, DataFillingException, RangingException, ParseIncrementalException, InterruptQueryException,
    NecessaryCollectionEmptyException, UICheckTimeoutException, FormulaException {
    int containerCount = containers.size();
    long t1 = System.currentTimeMillis();
    checkUITableConcurrently(containers);
    long t2 = System.currentTimeMillis();
    LOGGER.info("[CHECK-POINT] - Check ui table using " + (t2 - t1) + " milliseconds.");

    t1 = System.currentTimeMillis();
    for (FormulaParameterContainer fpc : containers) {
      fpc.init();
    }
    t2 = System.currentTimeMillis();
    LOGGER.info("[CHECK-POINT] - Initing containers using " + (t2 - t1) + " milliseconds");

    LOGGER.info("Entrust job to optimizer.");
    GroupByQueryOptimizer optimizer = new GroupByQueryOptimizer(this.containers);
    t1 = System.currentTimeMillis();
    List<GroupByIdResult> idResultList = optimizer.doWork();
    t2 = System.currentTimeMillis();
    LOGGER.info("[CHECK-POINT] Whole query finished in - " + (t2 - t1) + " milliseconds");

    if (CollectionUtils.isEmpty(idResultList)) {
      throw new XQueryException("Id result is empty.");
    }
    Map<String, String> descriptionMap = new HashMap<String, String>(containerCount);
    Map<String, Map<Object, Number>> calculatedResult = new HashMap<String, Map<Object, Number>>(containerCount);

    t1 = System.currentTimeMillis();
    Map<Object, Number> calculatedResultOfIdResult;

    Object status;
    String id;
    Map<String, Object> statusMap = new HashMap<String, Object>(idResultList.size());
    for (GroupByIdResult idResult : idResultList) {
      id = idResult.getId();
      calculatedResultOfIdResult = idResult.calculate();
      calculatedResult.put(id, calculatedResultOfIdResult);
      descriptionMap.put(id, idResult.getDescription());
      status = idResult.getStatus();
      statusMap.put(id, status);
    }
    t2 = System.currentTimeMillis();
    LOGGER.info("[CHECK-POINT] Calculate - " + (t2 - t1) + " milliseconds");
    putMonitorInfo(new WIEvent(WIE_STR_TIMEUSE_CALCULATE, t2 - t1));

    GroupByType groupByType;
    List<FormulaParameterItem> items = containers.get(0).getItems();
    groupByType = ((GroupByFormulaParameterItem) items.get(0)).getGroupByType();
    return new GroupByQueryResult(calculatedResult, groupByType, filterString, orderBy, order, index, pageSize,
                                  descriptionMap, statusMap);
  }
}
