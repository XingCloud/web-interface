package com.xingcloud.webinterface.exec;

import static com.google.common.base.Strings.repeat;
import static com.xingcloud.basic.Constants.SEPARATOR_STRING_LOG;
import static com.xingcloud.log.ContentLogger.logDescriptor;
import static com.xingcloud.webinterface.enums.CommonQueryType.NATURAL;
import static com.xingcloud.webinterface.enums.CommonQueryType.NORMAL;
import static com.xingcloud.webinterface.enums.CommonQueryType.TOTAL;
import static com.xingcloud.webinterface.monitor.SystemMonitor.putMonitorInfo;
import static com.xingcloud.webinterface.monitor.WIEvent.WIE_STR_TIMEUSE_INTEGRATE_RESULT;
import static com.xingcloud.webinterface.utils.IdResultBuilder.buildCommonDescriptor;

import com.xingcloud.maincache.InterruptQueryException;
import com.xingcloud.memcache.MemCacheException;
import com.xingcloud.webinterface.enums.CacheState;
import com.xingcloud.webinterface.enums.Interval;
import com.xingcloud.webinterface.exception.DataFillingException;
import com.xingcloud.webinterface.exception.FormulaException;
import com.xingcloud.webinterface.exception.NecessaryCollectionEmptyException;
import com.xingcloud.webinterface.exception.ParseIncrementalException;
import com.xingcloud.webinterface.exception.SegmentException;
import com.xingcloud.webinterface.exception.XParameterException;
import com.xingcloud.webinterface.exception.XQueryException;
import com.xingcloud.webinterface.model.ResultTuple;
import com.xingcloud.webinterface.model.formula.CommonFormulaQueryDescriptor;
import com.xingcloud.webinterface.model.formula.FormulaParameterContainer;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import com.xingcloud.webinterface.model.intermediate.CommonIdResult;
import com.xingcloud.webinterface.model.intermediate.CommonItemResult;
import com.xingcloud.webinterface.model.intermediate.CommonItemResultGroup;
import com.xingcloud.webinterface.monitor.WIEvent;
import com.xingcloud.webinterface.thread.XQueryExecutorServiceProvider;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.log4j.Logger;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;

public class CommonQueryOptimizer extends CheckCacheOptimizer {
  private static final Logger LOGGER = Logger.getLogger(CommonQueryOptimizer.class);
  private List<FormulaParameterContainer> containers;

  public CommonQueryOptimizer() {
  }

  public CommonQueryOptimizer(List<FormulaParameterContainer> containers) {
    this.containers = containers;
  }

  public List<CommonIdResult> doWork() throws XParameterException, SegmentException, XQueryException, ParseException,
    DataFillingException, ParseIncrementalException, InterruptQueryException, NecessaryCollectionEmptyException,
    MemCacheException, FormulaException {
    // 计时用
    long t1, t2;
    FormulaQueryDescriptor existsDescriptor;

    List<CommonIdResult> idResultList = new ArrayList<CommonIdResult>(containers.size());
    CommonIdResult idResult;
    Collection<FormulaQueryDescriptor> distinctDescriptors;

    t1 = System.currentTimeMillis();
    Map<FormulaQueryDescriptor, FormulaQueryDescriptor> m = null;
    for (FormulaParameterContainer fpc : containers) {
      idResult = buildCommonDescriptor(fpc);
      if (LOGGER.isDebugEnabled()) {
        logIdResultDebug(idResult);
      }
      logIdResultContent(idResult);

      idResultList.add(idResult);
      distinctDescriptors = idResult.distinctDescriptor();
      if (CollectionUtils.isEmpty(distinctDescriptors)) {
        continue;
      }
      if (m == null) {
        m = new HashMap<FormulaQueryDescriptor, FormulaQueryDescriptor>();
      }
      for (FormulaQueryDescriptor descriptor : distinctDescriptors) {
        if (descriptor.isKilled()) {
          continue;
        }
        existsDescriptor = m.get(descriptor);
        if (existsDescriptor == null) {
          m.put(descriptor, descriptor);
        } else {
          existsDescriptor.addFunctions(descriptor.getFunctions());
        }
      }
    }
    distinctDescriptors = MapUtils.isEmpty(m) ? null : m.values();
    t2 = System.currentTimeMillis();
    LOGGER.info("[OPTIMIZER] - Build descriptors and distinguish them use " + (t2 - t1) + " milliseconds");

    Map<FormulaQueryDescriptor, CacheState> descriptorStateMap = new HashMap<FormulaQueryDescriptor, CacheState>();

    // 查缓存
    Map<FormulaQueryDescriptor, Map<Object, ResultTuple>> descriptorTupleMap = checkCache(distinctDescriptors,
                                                                                          descriptorStateMap);

    if (CollectionUtils.isEmpty(distinctDescriptors)) {
      LOGGER.info("[OPTIMIZER] - All cache are hit, there is no any descriptors in query queue, ignore querying");
    } else {
      LOGGER.info("[OPTIMIZER] - Not all cache are hit, continue querying");
      ExecutorService service = XQueryExecutorServiceProvider.getService();
      //需要查询的请求一并提交，Query Master会对logical plan做合并和优化
      service.execute(new QueueBatchQueryTask(distinctDescriptors));
    }

    t1 = System.currentTimeMillis();
    for (CommonIdResult cir : idResultList) {
      cir.integrateResult(descriptorTupleMap, descriptorStateMap);
    }
    t2 = System.currentTimeMillis();
    LOGGER.info("[OPTIMIZER] - Integrate result using " + (t2 - t1) + " milliseconds");
    putMonitorInfo(new WIEvent(WIE_STR_TIMEUSE_INTEGRATE_RESULT, (t2 - t1)));
    return idResultList;
  }

  private Collection<FormulaQueryDescriptor> separate(Collection<FormulaQueryDescriptor> source) {
    Collection<FormulaQueryDescriptor> splitSet = null;
    CommonFormulaQueryDescriptor descriptor;
    Interval interval;
    Iterator<FormulaQueryDescriptor> it = source.iterator();
    FormulaQueryDescriptor fqd;
    while (it.hasNext()) {
      fqd = it.next();
      if (fqd != null && fqd.isCommon()) {
        descriptor = (CommonFormulaQueryDescriptor) fqd;
        interval = descriptor.getInterval();
        if (interval.getDays() >= 1) {
          if (splitSet == null) {
            splitSet = new HashSet<FormulaQueryDescriptor>();
          }
          splitSet.add(descriptor);
          it.remove();
        }
      }
    }
    return splitSet;
  }

  private void logIdResultDebug(CommonIdResult idResult) {
    if (idResult == null) {
      return;
    }
    Map<String, CommonItemResult> commonItemResultMap = idResult.getItemResultMap();
    if (MapUtils.isEmpty(commonItemResultMap)) {
      return;
    }
    String id = idResult.getId();
    String name;

    CommonItemResultGroup itemResultProxy;

    List<FormulaQueryDescriptor> descriptors;
    Collection<CommonItemResult> commonItemResults;
    LOGGER.debug("[" + id + "]");
    for (Entry<String, CommonItemResult> entry : commonItemResultMap.entrySet()) {
      name = entry.getKey();
      itemResultProxy = (CommonItemResultGroup) entry.getValue();
      LOGGER.debug(repeat(SEPARATOR_STRING_LOG, 1) + "[" + name + "] - TOTAL=" + itemResultProxy
        .getTotalAggregationPolicy() + ", NATURAL=" + itemResultProxy.getNaturalAggregationPolicy());
      commonItemResults = itemResultProxy.getCommonItemResults();

      if (CollectionUtils.isEmpty(commonItemResults)) {
        LOGGER.warn("[OPTIMIZER] - There is no any common item result for this proxy. There must be something wrong.");
        continue;
      }

      for (CommonItemResult itemResult : commonItemResults) {
        LOGGER.debug(repeat(SEPARATOR_STRING_LOG, 1) + "[" + NORMAL.name() + "]");
        descriptors = itemResult.getNormalConnectors();
        if (CollectionUtils.isEmpty(descriptors)) {
          LOGGER.warn("[OPTIMIZER] - There is no any connector for this item. There must be something wrong.");
          continue;
        }
        for (FormulaQueryDescriptor fqd : descriptors) {
          LOGGER.debug(repeat(SEPARATOR_STRING_LOG, 2) + fqd.toString());
        }
        LOGGER.debug(repeat(SEPARATOR_STRING_LOG, 1) + "[" + TOTAL.name() + "]");
        descriptors = itemResult.getTotalConnectors();
        if (CollectionUtils.isEmpty(descriptors)) {
          LOGGER.debug(repeat(SEPARATOR_STRING_LOG, 2) + "NO-CONTENT");
        } else {
          for (FormulaQueryDescriptor fqd : descriptors) {
            LOGGER.debug(repeat(SEPARATOR_STRING_LOG, 2) + fqd.toString());
          }
        }

        LOGGER.debug(repeat(SEPARATOR_STRING_LOG, 1) + "[" + NATURAL.name() + "]");
        descriptors = itemResult.getNaturalConnectors();
        if (CollectionUtils.isEmpty(descriptors)) {
          LOGGER.debug(repeat(SEPARATOR_STRING_LOG, 2) + "NO-CONTENT");
        } else {
          for (FormulaQueryDescriptor fqd : descriptors) {
            LOGGER.debug(repeat(SEPARATOR_STRING_LOG, 2) + fqd.toString());
          }
        }
      }
    }
  }

  private void logIdResultContent(CommonIdResult idResult) {
    if (idResult == null) {
      return;
    }
    Map<String, CommonItemResult> commonItemResultMap = idResult.getItemResultMap();
    if (MapUtils.isEmpty(commonItemResultMap)) {
      return;
    }
    CommonItemResultGroup itemResultProxy;

    List<FormulaQueryDescriptor> descriptors;
    Collection<CommonItemResult> commonItemResults;
    for (Entry<String, CommonItemResult> entry : commonItemResultMap.entrySet()) {
      itemResultProxy = (CommonItemResultGroup) entry.getValue();
      commonItemResults = itemResultProxy.getCommonItemResults();

      if (CollectionUtils.isEmpty(commonItemResults)) {
        continue;
      }

      for (CommonItemResult itemResult : commonItemResults) {
        descriptors = itemResult.getNormalConnectors();
        if (CollectionUtils.isEmpty(descriptors)) {
          continue;
        }
        for (FormulaQueryDescriptor fqd : descriptors) {
          logDescriptor(fqd);
        }
        descriptors = itemResult.getTotalConnectors();
        if (CollectionUtils.isNotEmpty(descriptors)) {
          for (FormulaQueryDescriptor fqd : descriptors) {
            logDescriptor(fqd);
          }
        }
        descriptors = itemResult.getNaturalConnectors();
        if (CollectionUtils.isNotEmpty(descriptors)) {
          for (FormulaQueryDescriptor fqd : descriptors) {
            logDescriptor(fqd);
          }
        }
      }
    }
  }
}
