package com.xingcloud.webinterface.utils;

import static com.xingcloud.webinterface.enums.CacheState.ACCURATE;
import static com.xingcloud.webinterface.model.Pending.isPendingPlaceholder;

import com.xingcloud.webinterface.enums.CacheState;
import com.xingcloud.webinterface.model.Pending;

public class IntermediateResultUtils {

  public static Object getStatusFromCacheState(CacheState cacheState) {
    return ACCURATE.equals(cacheState) ? null : Pending.INSTANCE;
  }

  public static Object spreadStatus(Object... subStatus) {
    for (Object o : subStatus) {
      if (isPendingPlaceholder(o)) {
        return o;
      }
    }
    return null;
  }

  public static Object spreadStatus(Object subStatus) {
    return isPendingPlaceholder(subStatus) ? subStatus : null;
  }
}
