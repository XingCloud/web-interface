package com.xingcloud.webinterface.exec;

import static com.xingcloud.webinterface.monitor.SystemMonitor.putMonitorInfo;
import static com.xingcloud.webinterface.monitor.WIEvent.WIE_STR_TIMEUSE_CALCULATE;
import static com.xingcloud.webinterface.monitor.WIEvent.WIE_STR_TIMEUSE_UI_CHECK;

import com.xingcloud.maincache.InterruptQueryException;
import com.xingcloud.memcache.MemCacheException;
import com.xingcloud.webinterface.enums.AggregationPolicyDisplayed;
import com.xingcloud.webinterface.exception.DataFillingException;
import com.xingcloud.webinterface.exception.FormulaException;
import com.xingcloud.webinterface.exception.NecessaryCollectionEmptyException;
import com.xingcloud.webinterface.exception.ParseIncrementalException;
import com.xingcloud.webinterface.exception.SegmentException;
import com.xingcloud.webinterface.exception.UICheckException;
import com.xingcloud.webinterface.exception.UICheckTimeoutException;
import com.xingcloud.webinterface.exception.XParameterException;
import com.xingcloud.webinterface.exception.XQueryException;
import com.xingcloud.webinterface.model.formula.FormulaParameterContainer;
import com.xingcloud.webinterface.model.intermediate.CommonIdResult;
import com.xingcloud.webinterface.model.result.CommonQueryResult;
import com.xingcloud.webinterface.model.result.QueryResult;
import com.xingcloud.webinterface.monitor.WIEvent;
import org.apache.commons.collections.CollectionUtils;
import org.apache.log4j.Logger;

import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CommonQueryExecutor extends AbstractQueryExecutor {
  private static final Logger LOGGER = Logger.getLogger(CommonQueryExecutor.class);

  private List<FormulaParameterContainer> containers;

  public CommonQueryExecutor(List<FormulaParameterContainer> containers) {
    this.containers = containers;
  }

  public QueryResult getResult() throws XQueryException, SegmentException, XParameterException, ParseException,
    UICheckException, DataFillingException, ParseIncrementalException, InterruptQueryException,
    NecessaryCollectionEmptyException, UICheckTimeoutException, FormulaException, MemCacheException {
    int containerCount = containers.size();
    long t1 = System.currentTimeMillis();
    checkUITableConcurrently(containers);
    long t2 = System.currentTimeMillis();
    putMonitorInfo(new WIEvent(WIE_STR_TIMEUSE_UI_CHECK, t2 - t1));
    LOGGER.info("[CHECK-POINT] - Check ui table using " + (t2 - t1) + " milliseconds.");

//    t1 = System.currentTimeMillis();
    for (FormulaParameterContainer fpc : containers) {
      fpc.init();
    }
//    t2 = System.currentTimeMillis();
//    LOGGER.info(
//        "[CHECK-POINT] - Initing containers using " + (t2 - t1) + " milliseconds");

    LOGGER.info("Entrust job to optimizer.");
    CommonQueryOptimizer optimizer = new CommonQueryOptimizer(this.containers);
//    t1 = System.currentTimeMillis();
    List<CommonIdResult> idResultList = optimizer.doWork();
//    t2 = System.currentTimeMillis();
//    LOGGER.info(
//      "[CHECK-POINT] Whole query finished in - " + (t2 - t1) + " milliseconds");

    if (CollectionUtils.isEmpty(idResultList)) {
      throw new XQueryException("Id result is empty.");
    }

    Map<String, Map<String, AggregationPolicyDisplayed>> summaryPolicyMap = new HashMap<String, Map<String, AggregationPolicyDisplayed>>(
      containerCount);
    Map<String, Object> statusMap = new HashMap<String, Object>(containerCount);
    Map<String, Map<Object, Number>> calculatedResult = new HashMap<String, Map<Object, Number>>(containerCount);

    t1 = System.currentTimeMillis();
    String id;
    for (CommonIdResult cir : idResultList) {
      id = cir.getId();
      calculatedResult.put(id, cir.calculate());
      statusMap.put(id, cir.getStatus());
      summaryPolicyMap.put(id, cir.getSummaryPolicyTypeMap());
    }
    t2 = System.currentTimeMillis();
    LOGGER.info("[CHECK-POINT] Calculate - " + (t2 - t1) + " milliseconds");
    putMonitorInfo(new WIEvent(WIE_STR_TIMEUSE_CALCULATE, t2 - t1));
    return new CommonQueryResult(calculatedResult, statusMap, summaryPolicyMap);
  }
}
