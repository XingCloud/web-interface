package com.xingcloud.webinterface.exec;

import static com.xingcloud.basic.Constants.SEPARATOR_CHAR_EVENT;
import static com.xingcloud.log.InaccurateCacheLogger.logCache;
import static com.xingcloud.webinterface.enums.CacheReference.OFFLINE;
import static com.xingcloud.webinterface.enums.CacheReference.ONLINE;
import static com.xingcloud.webinterface.enums.CacheState.ACCURATE;
import static com.xingcloud.webinterface.monitor.MonitorInfo.MI_STR_CACHE_OFFLINE;
import static com.xingcloud.webinterface.monitor.MonitorInfo.MI_STR_CACHE_ONLINE;
import static com.xingcloud.webinterface.monitor.MonitorInfo.MI_STR_DESCRIPTOR_MERGE;
import static com.xingcloud.webinterface.monitor.MonitorInfo.MI_STR_TIME_USE_CHECK_CACHE;
import static com.xingcloud.webinterface.monitor.SystemMonitor.putMonitorInfo;

import com.xingcloud.maincache.InterruptQueryException;
import com.xingcloud.webinterface.cache.StatefulCacheGetter;
import com.xingcloud.webinterface.cache.TimeUseAccumulator;
import com.xingcloud.webinterface.enums.CacheReference;
import com.xingcloud.webinterface.enums.CacheState;
import com.xingcloud.webinterface.exception.ParseIncrementalException;
import com.xingcloud.webinterface.model.ResultTuple;
import com.xingcloud.webinterface.model.StatefulCache;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import com.xingcloud.webinterface.monitor.MonitorInfo;
import com.xingcloud.webinterface.thread.XCacheGetExecutorServiceProvider;
import org.apache.commons.collections.CollectionUtils;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class CheckCacheOptimizer {

  private static final Logger LOGGER = Logger.getLogger(CheckCacheOptimizer.class);

  protected Map<FormulaQueryDescriptor, Map<Object, ResultTuple>> checkCache(
    Collection<FormulaQueryDescriptor> distinctDescriptors,
    Map<FormulaQueryDescriptor, CacheState> descriptorStateMap) throws ParseIncrementalException,
    InterruptQueryException {
    if (CollectionUtils.isEmpty(distinctDescriptors)) {
      return null;
    }

    int size = distinctDescriptors.size();

    Map<FormulaQueryDescriptor, Map<Object, ResultTuple>> descriptorTupleMap = new HashMap<FormulaQueryDescriptor, Map<Object, ResultTuple>>(
      size);

    StatefulCache sc;
    Map<Object, ResultTuple> cachedResultTupleMap;
    CacheState cs;

    Map<FormulaQueryDescriptor, Future<StatefulCache>> cacheFutureMap = new HashMap<FormulaQueryDescriptor, Future<StatefulCache>>(
      size);

    Future<StatefulCache> cacheFuture;
    ExecutorService service = XCacheGetExecutorServiceProvider.getService();

    TimeUseAccumulator accumulator = new TimeUseAccumulator();
    for (FormulaQueryDescriptor fqd : distinctDescriptors) {
      putMonitorInfo(new MonitorInfo(MI_STR_DESCRIPTOR_MERGE + SEPARATOR_CHAR_EVENT + fqd.getProjectId()));
      cacheFuture = service.submit(new StatefulCacheGetter(fqd, accumulator));
      cacheFutureMap.put(fqd, cacheFuture);
    }
    FormulaQueryDescriptor key;
    for (Entry<FormulaQueryDescriptor, Future<StatefulCache>> entry : cacheFutureMap.entrySet()) {
      key = entry.getKey();
      try {
        sc = entry.getValue().get();
      } catch (Exception e) {
        Throwable cause = e.getCause();
        if (cause instanceof ParseIncrementalException) {
          throw (ParseIncrementalException) cause;
        } else if (cause instanceof InterruptQueryException) {
          throw (InterruptQueryException) cause;
        } else {
          // Added @ 2013-02-17
          // 如果出了异常, 就不要再提交给后台查询了, 从集合里移除
          distinctDescriptors.remove(key);
          e.printStackTrace();
          continue;
        }
      }

      if (sc == null) {
        continue;
      }
      cs = sc.getState();
      cachedResultTupleMap = sc.getContent();
      if (cachedResultTupleMap == null) {
        continue;
      }
      if (descriptorStateMap == null) {
        descriptorStateMap = new HashMap<FormulaQueryDescriptor, CacheState>();
      }

      descriptorTupleMap.put(key, cachedResultTupleMap);
      descriptorStateMap.put(key, cs);
      if (ACCURATE.equals(cs)) {
        distinctDescriptors.remove(key);
      } else {
        logCache(cs, key);
      }

      CacheReference cr = sc.getReference();
      if (ONLINE.equals(cr)) {
        putMonitorInfo(new MonitorInfo(MI_STR_CACHE_ONLINE + SEPARATOR_CHAR_EVENT + key.getProjectId()));
      } else if (OFFLINE.equals(cr)) {
        putMonitorInfo(new MonitorInfo(MI_STR_CACHE_OFFLINE + SEPARATOR_CHAR_EVENT + key.getProjectId()));
      }
    }

    putMonitorInfo(new MonitorInfo(MI_STR_TIME_USE_CHECK_CACHE, accumulator.getRedisTotalTime()));

    LOGGER.info("[CHECK-POINT] Check cache - Redis used " + accumulator.getRedisTotalTime() + " milliseconds.");
    return descriptorTupleMap;
  }
}
