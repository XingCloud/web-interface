package com.xingcloud.webinterface.model.result;

import static com.xingcloud.webinterface.enums.CommonQueryType.NATURAL;
import static com.xingcloud.webinterface.enums.CommonQueryType.TOTAL;
import static com.xingcloud.webinterface.model.Pending.isPendingPlaceholder;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.NOT_AVAILABLE_STRING;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.PENDING_STRING;

import com.google.gson.Gson;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import com.xingcloud.webinterface.enums.AggregationPolicyDisplayed;
import com.xingcloud.webinterface.model.NotAvailableNumber;
import com.xingcloud.webinterface.model.PendingNumber;
import com.xingcloud.webinterface.utils.WebInterfaceConstants;
import com.xingcloud.webinterface.utils.comparator.ObjectArrayAscComparator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class CommonQueryResult extends QueryResult {

  public class SingleCommonQueryResult {

    @Expose
    private List<Object[]> data;

    @Expose
    private Object total;

    @Expose
    private Object natural;

    @Expose
    private Object status;

    @Expose
    @SerializedName("summarize_by")
    private Map<String, AggregationPolicyDisplayed> summaryPolicyTypeMap;

    public SingleCommonQueryResult(List<Object[]> data, Object total, Object natural,
                                   Map<String, AggregationPolicyDisplayed> summaryPolicyTypeMap) {
      super();
      this.data = data;
      this.total = total;
      this.natural = natural;
      this.summaryPolicyTypeMap = summaryPolicyTypeMap;
    }

    public SingleCommonQueryResult(List<Object[]> data, Object total, Object natural, Object status,
                                   Map<String, AggregationPolicyDisplayed> summaryPolicyTypeMap) {
      super();
      this.data = data;
      this.total = total;
      this.natural = natural;
      this.status = status;
      this.summaryPolicyTypeMap = summaryPolicyTypeMap;
    }

    public List<Object[]> getData() {
      return data;
    }

    public void setData(List<Object[]> data) {
      this.data = data;
    }

    public Object getTotal() {
      return total;
    }

    public void setTotal(Object total) {
      this.total = total;
    }

    public Object getNatural() {
      return natural;
    }

    public void setNatural(Object natural) {
      this.natural = natural;
    }

    public Object getStatus() {
      return status;
    }

    public void setStatus(Object status) {
      this.status = status;
    }

    public Map<String, AggregationPolicyDisplayed> getSummaryPolicyTypeMap() {
      return summaryPolicyTypeMap;
    }

    public void setSummaryPolicyTypeMap(Map<String, AggregationPolicyDisplayed> summaryPolicyTypeMap) {
      this.summaryPolicyTypeMap = summaryPolicyTypeMap;
    }
  }

  @Expose
  private Map<String, SingleCommonQueryResult> datas;

  private Map<String, Object> statusMap;

  private Map<String, Map<String, AggregationPolicyDisplayed>> summaryPolicyMap;

  public CommonQueryResult(Map<String, Map<Object, Number>> mapData) {
    super(mapData);
    format();
  }

  public CommonQueryResult(Map<String, Map<Object, Number>> mapData, Map<String, Object> statusMap,
                           Map<String, Map<String, AggregationPolicyDisplayed>> summaryPolicyMap) {
    super(mapData);
    this.statusMap = statusMap;
    this.summaryPolicyMap = summaryPolicyMap;
    format();
  }

  protected void format() {
    datas = new HashMap<String, SingleCommonQueryResult>(mapData.size());

    String id;
    Object total, natural, key, status;
    Number totalNumber, naturalNumber, queryValue;
    Map<String, AggregationPolicyDisplayed> spMap;
    SingleCommonQueryResult scqr;
    Map<Object, Number> valueMap;
    List<Object[]> objects;

    for (Entry<String, Map<Object, Number>> outerEntry : mapData.entrySet()) {
      id = outerEntry.getKey();
      valueMap = outerEntry.getValue();

      // 根据Total的值的不同, 写上合适的值
      key = TOTAL;
      totalNumber = valueMap.get(key);
      if (totalNumber instanceof NotAvailableNumber) {
        total = NOT_AVAILABLE_STRING;
      } else if (totalNumber instanceof PendingNumber) {
        total = PENDING_STRING;
      } else {
        total = totalNumber;
      }
      valueMap.remove(key);

      // 根据Natural的值的不同, 写上合适的值
      key = NATURAL;
      naturalNumber = valueMap.get(key);
      if (naturalNumber instanceof NotAvailableNumber) {
        natural = NOT_AVAILABLE_STRING;
      } else if (naturalNumber instanceof PendingNumber) {
        natural = PENDING_STRING;
      } else {
        natural = naturalNumber;
      }
      valueMap.remove(key);

      objects = new ArrayList<Object[]>(valueMap.size());
      for (Entry<Object, Number> innerEntry : valueMap.entrySet()) {
        queryValue = innerEntry.getValue();
        if (queryValue instanceof NotAvailableNumber) {
          objects.add(new Object[]{innerEntry.getKey(), NOT_AVAILABLE_STRING});
        } else if (queryValue instanceof PendingNumber) {
          objects.add(new Object[]{innerEntry.getKey(), PENDING_STRING});
        } else {
          objects.add(new Object[]{innerEntry.getKey(), queryValue});
        }
      }
      Collections.sort(objects, new ObjectArrayAscComparator());

      status = statusMap.get(id);
      spMap = summaryPolicyMap.get(id);
      if (isPendingPlaceholder(status)) {
        scqr = new SingleCommonQueryResult(objects, total, natural, PENDING_STRING, spMap);
      } else {
        scqr = new SingleCommonQueryResult(objects, total, natural, spMap);
      }
      datas.put(id, scqr);
    }
    result = true;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("CRQR.");
    sb.append(isResult());
    sb.append('.');
    sb.append(getMilli());
    sb.append('.');
    if (datas == null || datas.isEmpty()) {
      sb.append("NULL");
      return sb.toString();
    }

    Iterator<Entry<String, SingleCommonQueryResult>> it = datas.entrySet().iterator();
    Entry<String, SingleCommonQueryResult> entry;
    sb.append('[');
    for (; ; ) {
      entry = it.next();
      sb.append('(');
      sb.append(entry.getKey());
      sb.append('=');

      sb.append(')');
      if (!it.hasNext()) {
        break;
      }
      sb.append(", ");
    }
    sb.append(']');
    return sb.toString();
  }

  public static void main(String[] args) {
    Map<String, Map<Object, Number>> mapData = new HashMap<String, Map<Object, Number>>();
    Map<Object, Number> m1 = new HashMap<Object, Number>();
    m1.put("2012-09-10 00:00", 1l);
    m1.put("2012-09-11 00:00", 2l);
    m1.put("2012-09-12 00:00", 3l);
    m1.put(TOTAL.name(), 4l);
    m1.put(NATURAL.name(), 5l);
    mapData.put("001", m1);

    Map<Object, Number> m2 = new HashMap<Object, Number>();
    m2.put("2012-09-10 00:00", 11l);
    m2.put("2012-09-11 00:00", 22l);
    m2.put("2012-09-12 00:00", 33l);
    m2.put(TOTAL.name(), 44l);
    m2.put(NATURAL.name(), 55l);
    mapData.put("002", m2);

    QueryResult rqr = new CommonQueryResult(mapData);
    rqr.format();
    System.out.println(rqr);
    Gson g = WebInterfaceConstants.DEFAULT_GSON_PRETTY;
    System.out.println(g.toJson(rqr));
  }

}
