package com.xingcloud.webinterface.model.result;

import static com.google.common.base.Strings.repeat;
import static com.xingcloud.basic.Constants.SEPARATOR_STRING_LOG;
import static com.xingcloud.webinterface.enums.GroupByType.USER_PROPERTIES;
import static com.xingcloud.webinterface.model.Pending.isPendingPlaceholder;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.NOT_AVAILABLE_STRING;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.PENDING_STRING;

import com.google.common.base.Strings;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import com.xingcloud.webinterface.enums.GroupByType;
import com.xingcloud.webinterface.enums.Order;
import com.xingcloud.webinterface.model.NotAvailable;
import com.xingcloud.webinterface.model.NotAvailableNumber;
import com.xingcloud.webinterface.model.Pending;
import com.xingcloud.webinterface.model.PendingNumber;
import com.xingcloud.webinterface.utils.WebInterfaceConstants;
import com.xingcloud.webinterface.utils.comparator.GroupByKeyAscComparator;
import com.xingcloud.webinterface.utils.comparator.GroupByKeyDescComparator;
import com.xingcloud.webinterface.utils.comparator.GroupByValueAscComparator;
import com.xingcloud.webinterface.utils.comparator.GroupByValueDescComparator;
import com.xingcloud.webinterface.utils.range.XRange;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class GroupByQueryResult extends QueryResult {

  private static final Logger LOGGER = Logger.getLogger(GroupByQueryResult.class);
  @Expose
  private List<Object[]> datas;

  @Expose
  private int total;

  @Expose
  private Map<String, String> info;

  @Expose
  private List<String> keys;

  @Expose
  @SerializedName("status")
  private Map<String, Object> statusMap;

  private GroupByType groupByType;

  private String filterString;

  private String orderBy;

  private Order order;

  private int index;

  private int pageSize;

  public GroupByQueryResult(Map<String, Map<Object, Number>> mapData, GroupByType groupByType, String filterString,
                            String orderBy, Order order, int index, int pageSize, Map<String, String> info,
                            Map<String, Object> statusMap) {
    super(mapData);
    this.groupByType = groupByType;
    this.filterString = filterString;
    this.orderBy = orderBy;
    this.order = order;
    this.index = index;
    this.pageSize = pageSize;
    this.info = info;
    this.statusMap = statusMap;
    format();
  }

  public List<Object[]> getDatas() {
    return datas;
  }

  public void setDatas(List<Object[]> datas) {
    this.datas = datas;
  }

  public int getTotal() {
    return total;
  }

  public void setTotal(int total) {
    this.total = total;
  }

  public Map<String, String> getInfo() {
    return info;
  }

  public void setInfo(Map<String, String> info) {
    this.info = info;
  }

  public Map<String, Object> getStatusMap() {
    return statusMap;
  }

  @Override
  protected void format() {
    if (mapData == null || mapData.isEmpty()) {
      LOGGER.warn("[FORMATTER] - Empty input data map, exit intergrate.");
      return;
    }

    LOGGER.info("[FORMATTER] - Filtering, Making group by string union");
    Set<Object> groupByKeyUnion = new HashSet<Object>();
    Object tmpGroupBy;
    boolean usingRange = false, usingPagination = true;

    Map<Object, Number> map;
    Iterator<Entry<Object, Number>> it;
    Entry<Object, Number> e;

    Set<String> pendingIdSet = null, naIdSet = null;
    boolean hasPendingValue = false, hasNormalValue = false, hasNaValue = false;
    for (Entry<String, Map<Object, Number>> entry : mapData.entrySet()) {
      map = entry.getValue();
      if (map == null || map.isEmpty()) {
        LOGGER.info("[FORMATTER] - " + repeat(SEPARATOR_STRING_LOG, 1) + "Empty map, ignore(" + entry.getKey() + ").");
        continue;
      }

      it = map.entrySet().iterator();
      while (it.hasNext()) {
        e = it.next();
        tmpGroupBy = e.getKey();
        if (Pending.isPendingPlaceholder(tmpGroupBy)) {
          if (pendingIdSet == null) {
            pendingIdSet = new HashSet<String>();
          }
          hasPendingValue = true;
          pendingIdSet.add(entry.getKey());
          it.remove();
          continue;
        } else if (NotAvailable.isNotAvailablePlaceholder(tmpGroupBy)) {
          if (naIdSet == null) {
            naIdSet = new HashSet<String>();
          }
          hasNaValue = true;
          naIdSet.add(entry.getKey());
          it.remove();
          continue;
        } else if (!Strings.isNullOrEmpty(filterString) && tmpGroupBy != null && !tmpGroupBy.toString()
                                                                                            .contains(filterString)) {
          it.remove();
          continue;
        }
        if (tmpGroupBy instanceof XRange) {
          usingRange = true;
          usingPagination = false;
        }
        groupByKeyUnion.add(tmpGroupBy);
        hasNormalValue = true;
      }
    }
    if (!hasNormalValue) {
      if (hasPendingValue) {
        groupByKeyUnion.add(Pending.INSTANCE);
      } else if (hasNaValue) {
        groupByKeyUnion.add(NotAvailable.INSTANCE);
      }
    }
    int total = groupByKeyUnion.size();
    setTotal(total);

    LOGGER.info("[FORMATTER] - Padding placeholder.");
    Map<Object, Number> tmpMap;
    String id;
    for (Entry<String, Map<Object, Number>> entry : mapData.entrySet()) {
      id = entry.getKey();
      tmpMap = entry.getValue();
      if (tmpMap == null) {
        tmpMap = new HashMap<Object, Number>();
        mapData.put(id, tmpMap);
      }
      for (Object key : groupByKeyUnion) {
        if (!tmpMap.containsKey(key)) {
          if (pendingIdSet != null && pendingIdSet.contains(id)) {
            tmpMap.put(key, PendingNumber.INSTANCE);
          } else if (naIdSet != null && naIdSet.contains(id)) {
            tmpMap.put(key, NotAvailableNumber.INSTANCE);
          } else {
            tmpMap.put(key, NotAvailableNumber.INSTANCE);
          }
        }
      }
    }

    LOGGER.info("[FORMATTER] - Sorting, transform String to Number and transform map to array.");
    boolean defaultOrderBy = Strings.isNullOrEmpty(orderBy) || !mapData.containsKey(orderBy);

    Map<String, List<Object[]>> objectMap = new HashMap<String, List<Object[]>>(mapData.size());
    List<Object[]> tmpObjectArrayList, tmpStringFormattedArrayList;

    int from = 0, to = total - 1;
    if (usingPagination) {
      int[] fromTo = resolveFromTo(total, index, pageSize);
      from = fromTo[0];
      to = fromTo[1];
      LOGGER.info("[FORMATTER] - Using pagenation - items = [" + from + " to " + to + "]");
    } else {
      LOGGER.info("[FORMATTER] - Using pagenation - false");
      pageSize = total;
    }

    Object[] tmpObjectArray;
    // 默认排序方式, 按照key排序
    if (defaultOrderBy) {
      Comparator<Object[]> c;
      switch (order) {
        case ASC:
          c = new GroupByKeyAscComparator();
          break;
        default:
          c = new GroupByKeyDescComparator();
      }
      for (Entry<String, Map<Object, Number>> outerEntry : mapData.entrySet()) {
        id = outerEntry.getKey();
        tmpMap = outerEntry.getValue();
        if (tmpMap == null || tmpMap.isEmpty()) {
          continue;
        }
        tmpObjectArrayList = new ArrayList<Object[]>(total);
        // 按照key排序, 要先把key根据情况转换成数字型, 在排序
        if (!usingRange) {
          for (Entry<Object, Number> innerEntry : tmpMap.entrySet()) {
            tmpObjectArray = new Object[2];
            if (USER_PROPERTIES.equals(groupByType)) {
              tmpObjectArray[0] = toNumberIfCould(innerEntry.getKey());
            } else {
              tmpObjectArray[0] = innerEntry.getKey();
            }
            tmpObjectArray[1] = innerEntry.getValue();
            tmpObjectArrayList.add(tmpObjectArray);

          }
        } else {
          for (Entry<Object, Number> innerEntry : tmpMap.entrySet()) {
            tmpObjectArrayList.add(new Object[]{innerEntry.getKey(), innerEntry.getValue()});
          }
        }
        // 经过补占位符, 所有的id都拥有相同的key, 遍历的排序就可以了
        Collections.sort(tmpObjectArrayList, c);
        if (usingPagination) {
          objectMap.put(id, tmpObjectArrayList.subList(from, to + 1));
        } else {
          tmpStringFormattedArrayList = new ArrayList<Object[]>(tmpObjectArrayList.size());
          for (Object[] o : tmpObjectArrayList) {
            tmpStringFormattedArrayList.add(new Object[]{o[0].toString(), o[1]});
          }
          objectMap.put(id, tmpStringFormattedArrayList);
        }
      }
    }
    // 按照某个id指标的值排序
    else {
      tmpMap = mapData.get(orderBy);
      if (tmpMap != null && !tmpMap.isEmpty()) {
        // 因为不能通过value反向找到key是什么, 因此只能通过entry来识别key和value的对应关系
        Comparator<Entry<Object, Number>> c;
        switch (order) {
          case ASC:
            c = new GroupByValueAscComparator();
            break;
          default:
            c = new GroupByValueDescComparator();
        }
        // 基准线, 基准线的作用是, 把需要排序的列(某个id指标)的entry排好序后
        // 把entry的key按顺序放入基准线里, 以后其他的列(其他id指标)将按照这个基准线的顺序来放置值
        List<Entry<Object, Number>> entries = new ArrayList<Entry<Object, Number>>();
        for (Entry<Object, Number> entry : tmpMap.entrySet()) {
          entries.add(entry);
        }
        Collections.sort(entries, c);
        List<Object> baseline = new ArrayList<Object>(tmpMap.size());
        for (Entry<Object, Number> entry : entries) {
          baseline.add(entry.getKey());
        }
        // 确认了基准线后, 按照基准线的顺序开始构造值, 顺带完成分页
        for (Entry<String, Map<Object, Number>> entry : mapData.entrySet()) {
          tmpMap = entry.getValue();
          if (tmpMap == null || tmpMap.isEmpty()) {
            continue;
          }
          tmpObjectArrayList = new ArrayList<Object[]>(pageSize);
          for (int i = 0; i < baseline.size(); i++) {
            if (i >= to + 1) {
              break;
            }
            if (i >= from) {
              tmpObjectArray = new Object[2];
              if (!usingRange && USER_PROPERTIES.equals(groupByType)) {
                tmpObjectArray[0] = toNumberIfCould(baseline.get(i));
              } else {
                tmpObjectArray[0] = baseline.get(i);
              }
              tmpObjectArray[1] = tmpMap.get(baseline.get(i));
              tmpObjectArrayList.add(tmpObjectArray);
            }
          }
          objectMap.put(entry.getKey(), tmpObjectArrayList);
        }
      }
    }
    LOGGER.info("[FORMATTER] - Transposition.");
    // transposition
    int maxItemCount = total == 0 ? 0 : to - from + 1;
    List<Object[]> objectArrayList = new ArrayList<Object[]>(maxItemCount);
    Map<Object, Object> m;
    Object groupBy, value, status;
    Object[] o, existsObjectArray;
    List<Object[]> outerDataList;

    for (int i = 0; i < maxItemCount; i++) {
      o = new Object[2];
      for (Entry<String, List<Object[]>> entry : objectMap.entrySet()) {
        id = entry.getKey();
        outerDataList = entry.getValue();
        existsObjectArray = outerDataList.get(i);
        if (existsObjectArray == null || existsObjectArray.length != 2) {
          continue;
        }
        groupBy = existsObjectArray[0];
        value = existsObjectArray[1];
        if (groupBy instanceof Pending) {
          o[0] = PENDING_STRING;
        } else if (groupBy instanceof NotAvailable) {
          o[0] = NOT_AVAILABLE_STRING;
        } else if (groupBy instanceof XRange) {
          o[0] = groupBy.toString();
        } else {
          o[0] = groupBy;
        }

        if (o[1] == null) {
          o[1] = new HashMap<String, Object>();
        }
        m = (Map<Object, Object>) o[1];

        if (value == null) {
          m.put(id, null);
        } else if (value instanceof PendingNumber) {
          m.put(id, PENDING_STRING);
        } else if (value instanceof NotAvailableNumber) {
          m.put(id, NOT_AVAILABLE_STRING);
        } else {
          m.put(id, value);
        }
      }
      objectArrayList.add(o);
    }

    if (CollectionUtils.isNotEmpty(groupByKeyUnion)) {
      List<String> keys = new ArrayList<String>(groupByKeyUnion.size());
      for (Object obj : groupByKeyUnion) {
        keys.add(obj.toString());
        LOGGER.info("[FORMATTER] - Known key: " + obj);
      }
      Collections.sort(keys);
      setKeys(keys);
    }

    setDatas(objectArrayList);
    Map<String, Object> statusMap = getStatusMap();
    for (Entry<String, Object> entry : statusMap.entrySet()) {
      status = entry.getValue();
      if (isPendingPlaceholder(status)) {
        entry.setValue(PENDING_STRING);
      }
    }
  }

  public static Object toNumberIfCould(Object s) {
    try {
      if (s instanceof String && !Strings.isNullOrEmpty((String) s) && StringUtils.isNumeric((String) s)) {
        return NumberUtils.createLong((String) s);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return s;
  }

  public static int[] resolveFromTo(int total, int index, int pageSize) {
    int[] fromTo = new int[2];
    if (total == 0) {
      fromTo[0] = 0;
      fromTo[1] = 0;
      return fromTo;
    }
    if (pageSize <= 0) {
      pageSize = WebInterfaceConstants.PAGE_SIZE;
    }
    int pages;
    int lastPageCount = total % pageSize;
    if (lastPageCount == 0) {
      pages = total / pageSize;
    } else {
      pages = total / pageSize + 1;
    }

    if (index < 0) {
      index = WebInterfaceConstants.PAGE_INDEX_DEFAULT;
    }
    if (index >= pages) {
      index = pages - 1;
    }

    int from = (index) * pageSize;
    int to;

    if ((from + pageSize - 1) > total - 1) {
      to = from + lastPageCount - 1;
    } else {
      to = from + pageSize - 1;
    }
    fromTo[0] = from;
    fromTo[1] = to;
    return fromTo;
  }

  public void setGroupByType(GroupByType groupByType) {
    this.groupByType = groupByType;
  }

  public void setFilterString(String filterString) {
    this.filterString = filterString;
  }

  public void setOrderBy(String orderBy) {
    this.orderBy = orderBy;
  }

  public void setOrder(Order order) {
    this.order = order;
  }

  public void setIndex(int index) {
    this.index = index;
  }

  public void setPageSize(int pageSize) {
    this.pageSize = pageSize;
  }

  public void setKeys(List<String> keys) {
    this.keys = keys;
  }
}
