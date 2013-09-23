package com.xingcloud.webinterface.exec;

import static com.google.common.base.Strings.repeat;
import static com.xingcloud.basic.Constants.SEPARATOR_STRING_LOG;
import static com.xingcloud.log.ContentLogger.logDescriptor;
import static com.xingcloud.webinterface.conf.WebInterfaceConfig.BATCH_GROUPBY;
import static com.xingcloud.webinterface.enums.GroupByType.EVENT;
import static com.xingcloud.webinterface.exec.FormulaQueryDescriptorMerger.mergeDescriptor;
import static com.xingcloud.webinterface.monitor.MonitorInfo.MI_STR_TIME_USE_INTERGRATE_RESULT;
import static com.xingcloud.webinterface.monitor.SystemMonitor.putMonitorInfo;
import static com.xingcloud.webinterface.utils.IdResultBuilder.buildGroupByDescriptor;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.MAX_BATCH_DESCRIPTORS_IN_QUEUE;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.MAX_DESCRIPTORS_IN_QUEUE;

import com.google.common.base.Strings;
import com.xingcloud.maincache.InterruptQueryException;
import com.xingcloud.webinterface.enums.AggregationPolicy;
import com.xingcloud.webinterface.enums.GroupByType;
import com.xingcloud.webinterface.exception.DataFillingException;
import com.xingcloud.webinterface.exception.NecessaryCollectionEmptyException;
import com.xingcloud.webinterface.exception.ParseIncrementalException;
import com.xingcloud.webinterface.exception.RangingException;
import com.xingcloud.webinterface.exception.SegmentException;
import com.xingcloud.webinterface.exception.XParameterException;
import com.xingcloud.webinterface.exception.XQueryException;
import com.xingcloud.webinterface.model.EventAndFilterBEPair;
import com.xingcloud.webinterface.model.ResultTuple;
import com.xingcloud.webinterface.model.formula.FormulaParameterContainer;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import com.xingcloud.webinterface.model.formula.GroupByFormulaQueryDescriptor;
import com.xingcloud.webinterface.model.intermediate.GroupByIdResult;
import com.xingcloud.webinterface.model.intermediate.GroupByItemResult;
import com.xingcloud.webinterface.model.intermediate.GroupByItemResultGroup;
import com.xingcloud.webinterface.monitor.MonitorInfo;
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

public class GroupByQueryOptimizer extends CheckCacheOptimizer {
  private static final Logger LOGGER = Logger.getLogger(GroupByQueryOptimizer.class);
  private List<FormulaParameterContainer> containers;

  public GroupByQueryOptimizer() {
  }

  public GroupByQueryOptimizer(List<FormulaParameterContainer> containers) {
    this.containers = containers;
  }

  public List<GroupByIdResult> doWork() throws XParameterException, SegmentException, XQueryException, ParseException,
    DataFillingException, RangingException, ParseIncrementalException, InterruptQueryException,
    NecessaryCollectionEmptyException {
    // 计时用
    long t1, t2;
    FormulaQueryDescriptor existsDescriptor;
    List<GroupByIdResult> idResultList = new ArrayList<GroupByIdResult>(containers.size());
    Map<FormulaQueryDescriptor, Map<Object, ResultTuple>> descriptorTupleMap;
    GroupByIdResult idResult;
    Collection<FormulaQueryDescriptor> distinctDescriptors;
    Map<FormulaQueryDescriptor, FormulaQueryDescriptor> m = null;

    t1 = System.currentTimeMillis();
    for (FormulaParameterContainer fpc : containers) {
      idResult = buildGroupByDescriptor(fpc);

      if (LOGGER.isDebugEnabled()) {
        logIdResultDebug(idResult);
      } else {
        logIdResultContent(idResult);
      }

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
    LOGGER.info("[OPTIMIZER] - Building descriptors and distinguishing them using " + (t2 - t1) + " milliseconds");

    descriptorTupleMap = checkCache(distinctDescriptors, null);

    // 执行查询
    if (CollectionUtils.isEmpty(distinctDescriptors)) {
      LOGGER.info("[OPTIMIZER] - All cache are hit, there is no any descriptors in query queue, ignore querying");
    } else {
      ExecutorService service = XQueryExecutorServiceProvider.getService();
      if (BATCH_GROUPBY) {
        LOGGER.info("[OPTIMIZER] - Not all cache hit, continue querying batch.");

        //将EVENT分离出来, 他不能放在batch里执行
        Collection<FormulaQueryDescriptor> eventGroupByDescriptors = separate(distinctDescriptors);

        // 使用Merger对相同性质的查询合并(合并首尾衔接的日期)
        Map<EventAndFilterBEPair, Collection<FormulaQueryDescriptor>> mergedDescriptorMap = mergeDescriptor(
          distinctDescriptors);
        EventAndFilterBEPair pair;
        Collection<FormulaQueryDescriptor> pairSet;

        // 单纯的为了输出好看而单独打印
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Pair info summary");
          for (Entry<EventAndFilterBEPair, Collection<FormulaQueryDescriptor>> entry : mergedDescriptorMap.entrySet()) {
            pair = entry.getKey();
            pairSet = entry.getValue();
            LOGGER.debug(SEPARATOR_STRING_LOG + "[PAIR] - " + pair);
            for (FormulaQueryDescriptor fqd : pairSet) {
              LOGGER.debug(Strings.repeat(SEPARATOR_STRING_LOG, 2) + "[DESCRIPTOR] - " + fqd);
            }
          }
        }
        int counter = 0;
        if (CollectionUtils.isNotEmpty(eventGroupByDescriptors)) {
          LOGGER.info("[OPTIMIZER] - Submit event group-by descriptors(Single).");
          for (FormulaQueryDescriptor d : eventGroupByDescriptors) {
            ++counter;
            if (counter >= MAX_DESCRIPTORS_IN_QUEUE) {
              LOGGER.warn("[OPTIMIZER] - Overload, submit next round " + d);
              continue;
            }
            service.execute(new QueueSingleQueryTask(d));
          }
        }

        counter = 0;
        if (MapUtils.isNotEmpty(mergedDescriptorMap)) {
          LOGGER.info("[OPTIMIZER] - Submit user properties group-by descriptors(Batch).");
          for (Entry<EventAndFilterBEPair, Collection<FormulaQueryDescriptor>> entry : mergedDescriptorMap.entrySet()) {
            ++counter;
            pairSet = entry.getValue();
            if (counter >= MAX_BATCH_DESCRIPTORS_IN_QUEUE) {
              LOGGER.warn("[OPTIMIZER] - Overload, submit next round " + entry.getKey());
              continue;
            }
            service.execute(new QueueBatchQueryTask(pairSet));
          }
        }
      } else {
        LOGGER.info("[OPTIMIZER] - Not all cache hit, continue querying one by one.");
        int counter = 0;
        for (FormulaQueryDescriptor d : distinctDescriptors) {
          ++counter;
          if (counter >= MAX_DESCRIPTORS_IN_QUEUE) {
            LOGGER.warn("[OPTIMIZER] - Overload, submit next round " + d);
            continue;
          }
          service.execute(new QueueSingleQueryTask(d));
        }
      }
    }

    t1 = System.currentTimeMillis();
    for (GroupByIdResult groupByIdResult : idResultList) {
      groupByIdResult.integrateResult(descriptorTupleMap, null);
    }
    t2 = System.currentTimeMillis();
    LOGGER.info("[OPTIMIZER] - Integrate result using " + (t2 - t1) + " milliseconds");

    putMonitorInfo(new MonitorInfo(MI_STR_TIME_USE_INTERGRATE_RESULT, (t2 - t1)));
    return idResultList;
  }

  private Collection<FormulaQueryDescriptor> separate(Collection<FormulaQueryDescriptor> source) {
    if (CollectionUtils.isEmpty(source)) {
      return null;
    }
    Collection<FormulaQueryDescriptor> splitSet = null;
    GroupByFormulaQueryDescriptor descriptor;
    GroupByType groupByType;
    Iterator<FormulaQueryDescriptor> it = source.iterator();
    FormulaQueryDescriptor fqd;
    while (it.hasNext()) {
      fqd = it.next();
      if (fqd == null || !fqd.isGroupBy()) {
        continue;
      }
      descriptor = (GroupByFormulaQueryDescriptor) fqd;
      groupByType = descriptor.getGroupByType();
      if (EVENT.equals(groupByType)) {
        if (splitSet == null) {
          splitSet = new HashSet<FormulaQueryDescriptor>();
        }
        splitSet.add(descriptor);
        it.remove();
      }
    }
    return splitSet;
  }

  private void logIdResultDebug(GroupByIdResult idResult) {
    if (idResult == null) {
      return;
    }
    Map<String, GroupByItemResult> groupByItemResultMap = idResult.getItemResultMap();
    if (MapUtils.isEmpty(groupByItemResultMap)) {
      return;
    }
    String id = idResult.getId();
    String slicePattern = idResult.getSlicePattern();
    String name;
    AggregationPolicy ap;
    GroupByItemResultGroup itemResultProxy;
    List<FormulaQueryDescriptor> connector;
    Collection<GroupByItemResult> groupByItemResults;
    LOGGER.debug("[" + id + "]");
    LOGGER.debug(repeat(SEPARATOR_STRING_LOG, 1) + "[SLICE-PATTERN] - " + slicePattern);
    for (Entry<String, GroupByItemResult> entry : groupByItemResultMap.entrySet()) {
      name = entry.getKey();
      LOGGER.debug(repeat(SEPARATOR_STRING_LOG, 1) + "[" + name + "]");
      itemResultProxy = (GroupByItemResultGroup) entry.getValue();
      groupByItemResults = itemResultProxy.getGroupByItemResults();

      if (CollectionUtils.isEmpty(groupByItemResults)) {
        LOGGER
          .warn("[OPTIMIZER] - There is no any group-by item result for this proxy. There must be something wrong.");
        continue;
      }

      for (GroupByItemResult itemResult : groupByItemResults) {
        ap = itemResult.getGroupByAggregationPolicy();
        connector = itemResult.getConnector();
        if (CollectionUtils.isEmpty(connector)) {
          LOGGER.warn("There is no any connector for this item. There must be something wrong.");
          continue;
        }
        LOGGER.debug(repeat(SEPARATOR_STRING_LOG, 1) + "[AggregationPolicy] - " + ap +
                       " - [NeedCheckIntersection] - " + ap.needCheckIntersection());
        LOGGER.debug(repeat(SEPARATOR_STRING_LOG, 1) + "[CONNECTOR]");
        for (FormulaQueryDescriptor fqd : connector) {
          LOGGER.debug(repeat(SEPARATOR_STRING_LOG, 2) + fqd);
        }
      }
    }
  }

  private void logIdResultContent(GroupByIdResult idResult) {
    if (idResult == null) {
      return;
    }
    Map<String, GroupByItemResult> groupByItemResultMap = idResult.getItemResultMap();
    if (MapUtils.isEmpty(groupByItemResultMap)) {
      return;
    }
    GroupByItemResultGroup itemResultProxy;
    List<FormulaQueryDescriptor> connector;
    Collection<GroupByItemResult> groupByItemResults;
    for (Entry<String, GroupByItemResult> entry : groupByItemResultMap.entrySet()) {
      itemResultProxy = (GroupByItemResultGroup) entry.getValue();
      groupByItemResults = itemResultProxy.getGroupByItemResults();
      if (CollectionUtils.isEmpty(groupByItemResults)) {
        continue;
      }

      for (GroupByItemResult itemResult : groupByItemResults) {
        connector = itemResult.getConnector();
        if (CollectionUtils.isEmpty(connector)) {
          continue;
        }
        for (FormulaQueryDescriptor fqd : connector) {
          logDescriptor(fqd);
        }
      }
    }
  }
}
