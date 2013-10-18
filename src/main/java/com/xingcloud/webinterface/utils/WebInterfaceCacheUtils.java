package com.xingcloud.webinterface.utils;

import static com.xingcloud.webinterface.enums.CacheState.ACCURATE;
import static com.xingcloud.webinterface.enums.CacheState.EXPIRED;
import static com.xingcloud.webinterface.enums.CacheState.TOLERANT;
import static com.xingcloud.webinterface.enums.Function.COUNT;
import static com.xingcloud.webinterface.enums.Function.SUM;
import static com.xingcloud.webinterface.enums.Function.USER_NUM;

import com.xingcloud.webinterface.enums.CacheState;
import com.xingcloud.webinterface.model.ResultTuple;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @author Z J Wu @ 2012-8-21
 */
public class WebInterfaceCacheUtils {

  public static CacheState getCacheState(FormulaQueryDescriptor descriptor, boolean incremental, long timeElapse) {
    long dayDistance = descriptor.getDayDistance();
    if (incremental) {
      return ACCURATE;
    } else {
      long strict, loose;
      long numberOf7Days = dayDistance / 7;

      if (numberOf7Days == 0) {
        strict = 300;
        loose = strict * 36;
      } else {
        strict = 7200 * numberOf7Days;
        loose = strict * 2;
      }
      if (timeElapse < strict) {
        return ACCURATE;
      } else if (timeElapse >= strict && timeElapse < loose) {
        return TOLERANT;
      } else {
        return EXPIRED;
      }
    }
  }

  public static boolean isUseful(FormulaQueryDescriptor fqd, Map<Object, ResultTuple> map) {
    if (map == null || map.isEmpty()) {
      return false;
    }
    ResultTuple rt = null;

    boolean needCount = fqd.containsFunction(COUNT);
    boolean needSum = fqd.containsFunction(SUM);
    boolean needUserNum = fqd.containsFunction(USER_NUM);

    for (Entry<Object, ResultTuple> entry : map.entrySet()) {
      rt = entry.getValue();
      if (rt != null) {
        if ((needCount && rt.getCount() == null) || (needSum && rt.getSum() == null
        ) || (needUserNum && rt.getUsernum() == null)) {
          return false;
        } else {
          return true;
        }
      }
    }
    return false;
  }

  public static Map<Object, ResultTuple> cacheMap2ResultTuple(Map<String, Number[]> data) {
    if (data == null) {
      return null;
    }
    if (data.isEmpty()) {
      return new HashMap<Object, ResultTuple>(0);
    }
    Map<Object, ResultTuple> result = new HashMap<Object, ResultTuple>(data.size());
    Object key;
    ResultTuple rt;
    Number[] numbers;
    Number count, sum, userNum, samplingRate;
    for (Entry<String, Number[]> entry : data.entrySet()) {
      key = entry.getKey();
      numbers = entry.getValue();
      if (numbers == null) {
        result.put(key, null);
        continue;
      }
      count = numbers[0] == null ? null : numbers[0].longValue();
      sum = numbers[1] == null ? null : numbers[1].longValue();
      userNum = numbers[2] == null ? null : numbers[2].longValue();
      samplingRate = numbers[3] == null ? 1d : numbers[3].doubleValue();
      rt = new ResultTuple(count, sum, userNum, samplingRate.doubleValue());
      result.put(key, rt);
    }
    return result;
  }

}
