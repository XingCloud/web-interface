package com.xingcloud.webinterface.cache;

import static com.xingcloud.basic.Constants.DEFAULT_TIME_ZONE;
import static com.xingcloud.basic.mail.XMail.sendNewExceptionMail;
import static com.xingcloud.basic.utils.DateUtils.timeElapse2String;
import static com.xingcloud.webinterface.enums.CacheReference.OFFLINE;
import static com.xingcloud.webinterface.enums.CacheReference.ONLINE;
import static com.xingcloud.webinterface.enums.CacheState.EXPIRED;
import static com.xingcloud.webinterface.enums.Interval.PERIOD;
import static com.xingcloud.webinterface.model.ResultTuple.createNewEmptyResultTuple;
import static com.xingcloud.webinterface.utils.ModelUtils.isIncremental;
import static com.xingcloud.webinterface.utils.WebInterfaceCacheUtils.cacheMap2ResultTuple;
import static com.xingcloud.webinterface.utils.WebInterfaceCacheUtils.getCacheState;
import static com.xingcloud.webinterface.utils.WebInterfaceCacheUtils.isUseful;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.INCREMENTAL_STRING;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.VOLATILE_STRING;

import com.google.common.base.Strings;
import com.xingcloud.basic.utils.DateUtils;
import com.xingcloud.maincache.InterruptQueryException;
import com.xingcloud.maincache.MapXCache;
import com.xingcloud.maincache.XCacheException;
import com.xingcloud.maincache.XCacheOperator;
import com.xingcloud.maincache.redis.RedisXCacheOperator;
import com.xingcloud.webinterface.enums.CacheReference;
import com.xingcloud.webinterface.enums.CacheState;
import com.xingcloud.webinterface.enums.Interval;
import com.xingcloud.webinterface.exception.ParseIncrementalException;
import com.xingcloud.webinterface.model.ResultTuple;
import com.xingcloud.webinterface.model.StatefulCache;
import com.xingcloud.webinterface.model.formula.CommonFormulaQueryDescriptor;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import org.apache.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

public class RedisCacheChecker implements CacheChecker {

  public static CacheChecker INSTANCE;

  private RedisCacheChecker() {
  }

  public static synchronized CacheChecker getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new RedisCacheChecker();
    }
    return INSTANCE;
  }

  private static final Logger LOGGER = Logger.getLogger(RedisCacheChecker.class);

  @Override
  public StatefulCache checkCache(FormulaQueryDescriptor descriptor) throws XCacheException, ParseIncrementalException,
    InterruptQueryException {
    String key = descriptor.getKey();
    if (Strings.isNullOrEmpty(key)) {
      return null;
    }

    // 获取cache接口
    XCacheOperator cacheOperator = RedisXCacheOperator.getInstance();
    MapXCache cache;
    try {
      cache = cacheOperator.getMapCache(key);
    } catch (XCacheException e) {
      sendNewExceptionMail(e);
      throw e;
    }

    if (cache == null) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("[CACHE] - MISSED - " + key);
      }
      return null;
    }

    // 查询Cache
    Map<String, Number[]> numberMap = cache.toNumberArrayMap();
    if (numberMap == null) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("[CACHE] - HIT, NULL-CONTENT - " + key);
      }
      return null;
    }

    boolean incremental;
    String projectId = descriptor.getProjectId();
    String realBeginDate = descriptor.getRealBeginDate();
    String realEndDate = descriptor.getRealEndDate();
    String segment = descriptor.getSegment();

    long cacheTimestamp = cache.getTimestamp();
    if (cacheTimestamp == 0) {
      incremental = true;
    } else {
      try {
        incremental = isIncremental(cacheTimestamp, realBeginDate, realEndDate, projectId, segment);
      } catch (ParseIncrementalException e) {
        throw e;
      }
    }

    String incrementalString = incremental ? INCREMENTAL_STRING : VOLATILE_STRING;
    long milliseconds = Math.abs(System.currentTimeMillis() - cacheTimestamp);
    long seconds = milliseconds / 1000;
    String timeElapseString = timeElapse2String(milliseconds);

    CacheState cacheStatus = getCacheState(descriptor, incremental, seconds);
    if (EXPIRED.equals(cacheStatus)) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
          "[CACHE] - HIT, EXPIRED, " + incrementalString + " - " + key + " - " + timeElapseString + " elapsed(" + milliseconds + ").");
      }
      return null;
    }

    // 形态转换
    Map<Object, ResultTuple> tupleMap = cacheMap2ResultTuple(numberMap);

    if (tupleMap.isEmpty()) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("[CACHE] - HIT, EMPTY - " + key);
      }
      if (descriptor.isCommon()) {
        try {
          CommonFormulaQueryDescriptor cfqd = (CommonFormulaQueryDescriptor) descriptor;
          return new StatefulCache(ONLINE, cacheStatus,
                                   buildPlaceHolderTupleMap(descriptor.getRealBeginDate(), cfqd.getInterval()),
                                   seconds);
        } catch (ParseException e) {
          throw new XCacheException(e);
        }
      } else {
        return new StatefulCache(ONLINE, cacheStatus, tupleMap, seconds);
      }
    }
    if (!isUseful(descriptor, tupleMap)) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("[CACHE] - HIT, USELESS - " + key);
      }
      return null;
    }
    CacheReference cr;
    if (cacheTimestamp == 0) {
      cr = OFFLINE;
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("[CACHE] - HIT, OFFLINE, " + incrementalString + " - " + key + " - " + cacheStatus.name());
      }
    } else {
      cr = ONLINE;
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("[CACHE] - HIT, ONLINE, " + incrementalString + " - " + key + " - " + cacheStatus
          .name() + " - " + timeElapseString + " elapsed(" + milliseconds + ").");
      }
    }
    return new StatefulCache(cr, cacheStatus, tupleMap, seconds);
  }

  private Map<Object, ResultTuple> buildPlaceHolderTupleMap(String date, Interval interval) throws ParseException {
    Map<Object, ResultTuple> resultTupleMapPlaceHolder;
    Calendar c = Calendar.getInstance(DEFAULT_TIME_ZONE);
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    sdf.setTimeZone(DEFAULT_TIME_ZONE);
    c.setTime(DateUtils.short2Date(date));
    switch (interval) {
      case HOUR:
        resultTupleMapPlaceHolder = new HashMap<Object, ResultTuple>(24);
        for (int i = 0; i < 24; i++) {
          resultTupleMapPlaceHolder.put(sdf.format(c.getTime()), createNewEmptyResultTuple());
          c.add(Calendar.HOUR, 1);
        }
        break;
      case MIN5:
        resultTupleMapPlaceHolder = new HashMap<Object, ResultTuple>(288);
        for (int i = 0; i < 288; i++) {
          resultTupleMapPlaceHolder.put(sdf.format(c.getTime()), createNewEmptyResultTuple());
          c.add(Calendar.MINUTE, 5);
        }
        break;
      default:
        resultTupleMapPlaceHolder = new HashMap<Object, ResultTuple>(1);
        resultTupleMapPlaceHolder.put(PERIOD.name(), createNewEmptyResultTuple());
        break;
    }
    return resultTupleMapPlaceHolder;
  }
}
