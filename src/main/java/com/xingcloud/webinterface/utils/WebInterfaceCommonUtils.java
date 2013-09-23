package com.xingcloud.webinterface.utils;

import com.google.common.base.Strings;
import com.xingcloud.webinterface.conf.WebInterfaceConfig;
import com.xingcloud.webinterface.thread.ThreadFactoryInfo;
import com.xingcloud.webinterface.utils.range.XRange;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

/**
 * Author: qiujiawei Date: 12-3-19
 */
public class WebInterfaceCommonUtils {
  private static final Logger LOGGER = Logger.getLogger(WebInterfaceCommonUtils.class);

  public static Map<String, ThreadFactoryInfo> THREAD_FACTORY_MAP;

  static {
    Configuration configuration = WebInterfaceConfig.getConfiguration();
    Collection collection = (Collection) configuration.getProperty("tfs.tf");

    if (CollectionUtils.isNotEmpty(collection)) {
      THREAD_FACTORY_MAP = new HashMap<String, ThreadFactoryInfo>(collection.size());
      ThreadFactoryInfo tfi;
      String wholeName, id, name;
      int threadCount, idx;
      for (Object o : collection) {
        wholeName = o.toString();
        idx = wholeName.indexOf(':');
        name = wholeName.substring(0, idx);
        id = name;
        threadCount = safeConvertInt(wholeName.substring(idx + 1), 10);
        tfi = new ThreadFactoryInfo(id, name, threadCount);
        THREAD_FACTORY_MAP.put(id, tfi);
        LOGGER.info("[THREAD-POOL] - " + tfi);
      }

    }
  }

  public static String safeGetStringFromProperties(Properties prop, String k, String defaultValue) {
    if (MapUtils.isEmpty(prop) || Strings.isNullOrEmpty(k)) {
      return defaultValue;
    }
    String v = prop.getProperty(k);
    return v == null ? defaultValue : v;
  }

  public static int safeConvertInt(String intString, int defaultValue) {
    if (!StringUtils.isNumeric(intString)) {
      return defaultValue;
    }
    return Integer.valueOf(intString);
  }

  public static boolean safeConvertBoolean(String booleanString, boolean defalutValue) {
    if (Strings.isNullOrEmpty(booleanString)) {
      return defalutValue;
    }
    if (booleanString.equalsIgnoreCase("true") || booleanString.equalsIgnoreCase("false")) {
      return Boolean.parseBoolean(booleanString);
    }
    return defalutValue;
  }

  public static <O, P> P safeGet(Map<O, P> map, O key) {
    if (MapUtils.isEmpty(map)) {
      return null;
    }
    return map.get(key);
  }

  public static int compareObject(Object o1, Object o2) {
    if (o1 == null && o2 == null) {
      return 0;
    } else if (o1 == null && o2 != null) {
      return 1;
    } else if (o1 != null && o2 == null) {
      return -1;
    }

    if (o1 instanceof Number && o2 instanceof String) {
      return -1;
    } else if (o1 instanceof String && o2 instanceof Number) {
      return 1;
    } else if (o1 instanceof Number && o2 instanceof Number) {
      double d1 = ((Number) o1).doubleValue();
      double d2 = ((Number) o2).doubleValue();
      if (d1 < d2) {
        return -1;
      } else if (d1 > d2) {
        return 1;
      }
      return 0;
    } else if (o1 instanceof XRange && o2 instanceof XRange) {
      int p1 = ((XRange) o1).getPosition();
      int p2 = ((XRange) o2).getPosition();
      if (p1 > p2) {
        return 1;
      } else if (p1 < p2) {
        return -1;
      }
      return 0;
    } else {
      return (o1.toString()).compareTo(o2.toString());
    }
  }

  public static int booleans2Int(boolean... booleans) {
    if (ArrayUtils.isEmpty(booleans)) {
      return 0;
    }
    int result = 0;
    for (int i = booleans.length - 1; i >= 0; i--) {
      result += (booleans[i] ? 1 : 0) << (booleans.length - (i + 1));
    }
    return result;
  }

  public static void appendStringBuilder(StringBuilder sb, Object o) {
    if (o == null) {
      return;
    }
    String s = o.toString();
    if (Strings.isNullOrEmpty(s)) {
      return;
    }
    sb.append(s);
    sb.append('.');
  }

  public static void put2MapIgnoreEmpty(Map<String, String> m, Object k, Object v) {
    if (k == null || v == null) {
      return;
    }
    String ks = k.toString();
    String vs = v.toString();
    if (Strings.isNullOrEmpty(ks) || Strings.isNullOrEmpty(vs)) {
      return;
    }
    m.put(ks, vs);
  }

  public static String truncateStar(String star) {
    if (star == null || !star.endsWith(".*.*")) {
      return star;
    }
    return truncateStar(star.substring(0, star.lastIndexOf(".*")));
  }

  public static int indexOf(String s, char c, int i) {
    int count = 0;
    for (int j = 0; j < s.length(); j++) {
      if (s.charAt(j) == c && ++count == i) {
        return j;
      }
    }
    return -1;
  }

  public static Set<String> trim2Last(Set<String> original, char c, int idx) {
    if (CollectionUtils.isEmpty(original)) {
      return null;
    }
    Set<String> set = new HashSet<String>();
    int last;
    for (String s : original) {
      last = WebInterfaceCommonUtils.indexOf(s, c, idx);
      if (last < 0) {
        set.add(s);
      } else {
        set.add(s.substring(0, last) + ".");
      }
    }
    return set;
  }

  public static String truncateAllStar(String star) {
    if (star == null || !star.endsWith(".*")) {
      return star;
    }
    return truncateAllStar(star.substring(0, star.lastIndexOf(".*")));
  }

  public static String[] split2LevelArray(String event) {
    if (Strings.isNullOrEmpty(event)) {
      return null;
    }
    event = truncateAllStar(event);
    String[] event6LevelArray = event.split("\\.");
    if (event6LevelArray == null || event6LevelArray.length == 0) {
      return null;
    }
    for (int i = 0; i < event6LevelArray.length; i++) {
      if ("*".equals(event6LevelArray[i])) {
        event6LevelArray[i] = null;
      }
    }
    return event6LevelArray;
  }

  public static long randomLong(long seed) {
    if (seed == 0) {
      seed = Long.MAX_VALUE;
    }
    return Math.round(Math.random() * seed);
  }

  public static <O, P, Q> Map<P, Map<O, Q>> flip(Map<O, Map<P, Q>> map) {
    if (map == null) {
      return null;
    }
    Map<P, Map<O, Q>> flippedMap = new HashMap<P, Map<O, Q>>();
    Map<O, Q> tmpMap;

    O outerKey;
    Map<P, Q> outerValue;

    P innerKey;
    Q value;

    for (Entry<O, Map<P, Q>> outerEntry : map.entrySet()) {
      outerKey = outerEntry.getKey();
      outerValue = outerEntry.getValue();
      if (outerValue == null || outerValue.isEmpty()) {
        continue;
      }
      for (Entry<P, Q> innerEntry : outerValue.entrySet()) {
        innerKey = innerEntry.getKey();
        value = innerEntry.getValue();
        tmpMap = flippedMap.get(innerKey);
        if (tmpMap == null) {
          tmpMap = new HashMap<O, Q>();
          flippedMap.put(innerKey, tmpMap);
        }
        tmpMap.put(outerKey, value);
      }
    }
    return flippedMap;
  }

  public static void main(String[] args) {
    byte b=2;
    short s=30000;
    byte[] bytes = ByteBuffer.allocate(2).putShort(s).array();
    System.out.println(Arrays.toString(bytes));
  }
}
