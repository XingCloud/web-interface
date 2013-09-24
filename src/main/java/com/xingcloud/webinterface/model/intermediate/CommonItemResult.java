package com.xingcloud.webinterface.model.intermediate;

import static com.xingcloud.basic.Constants.DEFAULT_TIME_ZONE;
import static com.xingcloud.basic.utils.DateUtils.date2Long;
import static com.xingcloud.basic.utils.DateUtils.getFullDisplayDateString;
import static com.xingcloud.basic.utils.DateUtils.short2Date;
import static com.xingcloud.webinterface.enums.AggregationPolicy.ACCUMULATION;
import static com.xingcloud.webinterface.enums.AggregationPolicy.ACCUMULATION_EXTEND;
import static com.xingcloud.webinterface.enums.AggregationPolicy.AVERAGE;
import static com.xingcloud.webinterface.enums.AggregationPolicy.AVERAGE_EXTEND;
import static com.xingcloud.webinterface.enums.AggregationPolicy.SAME_AS_QUERY;
import static com.xingcloud.webinterface.enums.AggregationPolicy.SAME_AS_TOTAL;
import static com.xingcloud.webinterface.enums.CommonQueryType.NATURAL;
import static com.xingcloud.webinterface.enums.CommonQueryType.TOTAL;
import static com.xingcloud.webinterface.enums.DateTruncateType.KILL;
import static com.xingcloud.webinterface.enums.Interval.HOUR;
import static com.xingcloud.webinterface.enums.Interval.MIN5;
import static com.xingcloud.webinterface.model.ResultTuple.NA_RESULT_TUPLE;
import static com.xingcloud.webinterface.model.ResultTuple.PENDING_RESULT_TUPLE;
import static com.xingcloud.webinterface.model.ResultTuple.createNewEmptyResultTuple;
import static com.xingcloud.webinterface.model.ResultTuple.createNewNullResultTuple;
import static com.xingcloud.webinterface.utils.IntermediateResultUtils.getStatusFromCacheState;
import static com.xingcloud.webinterface.utils.WebInterfaceCommonUtils.safeGet;

import com.xingcloud.webinterface.enums.AggregationPolicy;
import com.xingcloud.webinterface.enums.CacheState;
import com.xingcloud.webinterface.enums.Interval;
import com.xingcloud.webinterface.exception.DataFillingException;
import com.xingcloud.webinterface.exception.NecessaryCollectionEmptyException;
import com.xingcloud.webinterface.model.NotAvailable;
import com.xingcloud.webinterface.model.Pending;
import com.xingcloud.webinterface.model.ResultTuple;
import com.xingcloud.webinterface.model.formula.CommonFormulaQueryDescriptor;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.log4j.Logger;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class CommonItemResult extends ItemResult {

  private static final Logger LOGGER = Logger.getLogger(CommonItemResult.class);

  protected List<FormulaQueryDescriptor> normalConnectors;
  protected List<FormulaQueryDescriptor> totalConnectors;
  protected List<FormulaQueryDescriptor> naturalConnectors;

  // protected Map<CommonQueryType, List<FormulaQueryDescriptor>>
  // connectorMap;

  protected AggregationPolicy totalAggregationPolicy;

  protected AggregationPolicy naturalAggregationPolicy;

  protected KeyTuple totalAggregation;

  protected KeyTuple naturalAggregation;

  // 所有item共同维护的TOTAL汇总需要的key的交集, 需要在id级别指定
  protected Set<Object> totalKeyIntersection;
  // 所有item共同维护的NATURAL汇总需要的key的交集, 需要在id级别指定
  protected Set<Object> naturalKeyIntersection;

  public CommonItemResult(String name) {
    super(name);
  }

  public CommonItemResult(String name, AggregationPolicy totalAggregationPolicy,
                          AggregationPolicy naturalAggregationPolicy) {
    super(name);
    this.totalAggregationPolicy = totalAggregationPolicy;
    this.naturalAggregationPolicy = naturalAggregationPolicy;
  }

  public CommonItemResult(String name, List<FormulaQueryDescriptor> normalConnectors,
                          List<FormulaQueryDescriptor> totalConnectors, List<FormulaQueryDescriptor> naturalConnectors,
                          AggregationPolicy totalAggregationPolicy, AggregationPolicy naturalAggregationPolicy,
                          Set<Object> totalKeyIntersection, Set<Object> naturalKeyIntersection) {
    super(name);
    this.normalConnectors = normalConnectors;
    this.totalConnectors = totalConnectors;
    this.naturalConnectors = naturalConnectors;
    this.totalAggregationPolicy = totalAggregationPolicy;
    this.naturalAggregationPolicy = naturalAggregationPolicy;
    this.totalKeyIntersection = totalKeyIntersection;
    this.naturalKeyIntersection = naturalKeyIntersection;
  }

  public List<FormulaQueryDescriptor> getNormalConnectors() {
    return normalConnectors;
  }

  public List<FormulaQueryDescriptor> getTotalConnectors() {
    return totalConnectors;
  }

  public List<FormulaQueryDescriptor> getNaturalConnectors() {
    return naturalConnectors;
  }

  public AggregationPolicy getTotalAggregationPolicy() {
    return totalAggregationPolicy;
  }

  public AggregationPolicy getNaturalAggregationPolicy() {
    return naturalAggregationPolicy;
  }

  public KeyTuple getTotalAggregation() {
    return totalAggregation;
  }

  public void setTotalAggregation(KeyTuple totalAggregation) {
    this.totalAggregation = totalAggregation;
  }

  public KeyTuple getNaturalAggregation() {
    return naturalAggregation;
  }

  public void setNaturalAggregation(KeyTuple naturalAggregation) {
    this.naturalAggregation = naturalAggregation;
  }

  public Set<Object> getTotalKeyIntersection() {
    return totalKeyIntersection;
  }

  public Set<Object> getNaturalKeyIntersection() {
    return naturalKeyIntersection;
  }

  public void fillNormalResult(Map<FormulaQueryDescriptor, Map<Object, ResultTuple>> descriptorTupleMap,
                               Map<FormulaQueryDescriptor, CacheState> descriptorStateMap) throws DataFillingException {
    CommonFormulaQueryDescriptor cfqd;
    Interval interval;
    String displayDate;
    boolean makeNAValue;
    Calendar c = Calendar.getInstance();
    Map<Object, ResultTuple> tupleMap;
    CacheState cacheState;
    Object status;

    ResultTuple placeholder, rt;

    boolean checkTotalNormal = false, checkNaturalNormal = false, checkTotalSelf = false;
    String displayString;

    Set<Object> tupleKeySetOfThisItem = null;
    AggregationPolicy tap = getTotalAggregationPolicy(), nap = getNaturalAggregationPolicy();

    // ACCUMULATION, AVERAGE等于Total, 需要检查Normal查询结果
    if (ACCUMULATION.equals(tap) || AVERAGE.equals(tap)) {
      checkTotalNormal = true;
      if (tupleKeySetOfThisItem == null) {
        tupleKeySetOfThisItem = new HashSet<Object>();
      }
    }
    // ACCUMULATION_EXTEND, AVERAGE_EXTEND等于total, 需要检查total自身的查询结果
    else if (ACCUMULATION_EXTEND.equals(tap) || AVERAGE_EXTEND.equals(tap)) {
      checkTotalSelf = true;
    }
    // ACCUMULATION, AVERAGE等于Natural, 需要检查Normal查询结果
    if (nap.needCheckIntersection()) {
      checkNaturalNormal = true;
      if (tupleKeySetOfThisItem == null) {
        tupleKeySetOfThisItem = new HashSet<Object>();
      }
    }

    Set<KeyTuple> ktList = new HashSet<KeyTuple>();

    KeyTuple kt;
    // 填充Normal查询结果阶段
    for (FormulaQueryDescriptor fqd : this.normalConnectors) {
      tupleMap = safeGet(descriptorTupleMap, fqd);
      displayDate = fqd.getInputBeginDate();

      cfqd = (CommonFormulaQueryDescriptor) fqd;
      interval = cfqd.getInterval();

      try {
        c.setTime(short2Date(displayDate));
      } catch (ParseException e) {
        e.printStackTrace();
      }
      c.setTimeZone(DEFAULT_TIME_ZONE);

      if (tupleMap == null) {
        makeNAValue = fqd.isKilled();
        placeholder = (makeNAValue ? NA_RESULT_TUPLE : PENDING_RESULT_TUPLE);
        status = (makeNAValue ? null : Pending.INSTANCE);
        if (MIN5.equals(interval)) {
          for (int i = 0; i < 288; i++) {
            ktList.add(new KeyTuple(date2Long(c.getTime()), placeholder, status));
            c.add(Calendar.MINUTE, 5);
          }
        } else if (HOUR.equals(interval)) {
          for (int i = 0; i < 24; i++) {
            ktList.add(new KeyTuple(date2Long(c.getTime()), placeholder, status));
            c.add(Calendar.HOUR, 1);
          }
        } else {
          ktList.add(new KeyTuple(getFullDisplayDateString(displayDate), placeholder, status));
        }
      } else {
        // 如果查询结果有内容, 要根据情况添加check key set
        cacheState = descriptorStateMap.get(fqd);
        status = getStatusFromCacheState(cacheState);
        if (interval.getDays() < 1) {
          String s = null;
          // 如果Cache里面的数不足288或24个, 适当补零
          if (MIN5.equals(interval)) {
            for (int i = 0; i < 288; i++) {
              s = date2Long(c.getTime());
              rt = tupleMap.get(s);
              if (rt == null) {
                rt = createNewEmptyResultTuple();
              }
              ktList.add(new KeyTuple(s, rt, status));
              if (checkTotalNormal || checkNaturalNormal) {
                tupleKeySetOfThisItem.add(s);
              }
              c.add(Calendar.MINUTE, 5);
            }
          } else if (HOUR.equals(interval)) {
            for (int i = 0; i < 24; i++) {
              s = date2Long(c.getTime());
              rt = tupleMap.get(s);
              if (rt == null) {
                rt = createNewEmptyResultTuple();
              }
              ktList.add(new KeyTuple(s, rt, status));
              if (checkTotalNormal || checkNaturalNormal) {
                tupleKeySetOfThisItem.add(s);
              }
              c.add(Calendar.HOUR, 1);
            }
          } else {
            for (Entry<Object, ResultTuple> entry : tupleMap.entrySet()) {
              ktList.add(new KeyTuple(entry.getKey(), entry.getValue(), status));
              if (checkTotalNormal || checkNaturalNormal) {
                tupleKeySetOfThisItem.add(entry.getKey());
              }
            }
          }
        } else {
          displayString = getFullDisplayDateString(displayDate);
          if (checkTotalNormal || checkNaturalNormal) {
            tupleKeySetOfThisItem.add(displayString);
          }
          for (Entry<Object, ResultTuple> entry : tupleMap.entrySet()) {
            kt = new KeyTuple(displayString, displayString, entry.getValue(), status);
            ktList.add(kt);
          }
        }
      }
    }

    setNormalResult(ktList);
    // 取交集-Total
    Set<Object> s;
    if (checkTotalNormal && CollectionUtils.isNotEmpty(tupleKeySetOfThisItem)) {
      s = getTotalKeyIntersection();
      if (CollectionUtils.isEmpty(s)) {
        s.addAll(tupleKeySetOfThisItem);
      } else {
        s.retainAll(tupleKeySetOfThisItem);
      }
    }
    // 取交集-Total自己的Descriptor
    if (checkTotalSelf) {
      s = getTotalKeyIntersection();
      Set<Object> totalSelfKeys = prepareExtendTotalAggregationKeys(descriptorTupleMap);
      if (CollectionUtils.isNotEmpty(totalSelfKeys)) {
        if (CollectionUtils.isEmpty(s)) {
          s.addAll(totalSelfKeys);
        } else {
          s.retainAll(totalSelfKeys);
        }
      }
    }

    // 取交集-Natural
    if (checkNaturalNormal && CollectionUtils.isNotEmpty(tupleKeySetOfThisItem)) {
      s = getNaturalKeyIntersection();
      if (CollectionUtils.isEmpty(s)) {
        s.addAll(tupleKeySetOfThisItem);
      } else {
        s.retainAll(tupleKeySetOfThisItem);
      }
    }

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
        "[COMMON-ITEM-RESULT] - Info of total & natural summary is standby(" + name + ", make intersection if necessary).");
    }
  }

  private Set<Object> prepareExtendTotalAggregationKeys(
    Map<FormulaQueryDescriptor, Map<Object, ResultTuple>> descriptorTupleMap) {
    Map<Object, ResultTuple> m;
    Set<Object> keys = null;
    CommonFormulaQueryDescriptor cfqd;
    Interval interval;
    for (FormulaQueryDescriptor fqd : this.totalConnectors) {
      if (KILL.equals(fqd.getDateTruncateType())) {
        continue;
      }
      cfqd = (CommonFormulaQueryDescriptor) fqd;
      interval = cfqd.getInterval();

      m = safeGet(descriptorTupleMap, fqd);
      if (MapUtils.isEmpty(m)) {
        continue;
      }
      if (keys == null) {
        keys = new HashSet<Object>();
      }
      if (interval.getDays() < 1) {
        keys.addAll(m.keySet());
      } else {
        keys.add(getFullDisplayDateString(fqd.getInputBeginDate()));
      }
    }
    return keys;
  }

  public void fillTotalAggregation(Map<FormulaQueryDescriptor, Map<Object, ResultTuple>> descriptorTupleMap,
                                   Map<FormulaQueryDescriptor, CacheState> descriptorStateMap) {
    Object totalStatus = null;
    Object status;
    CacheState cacheState;
    AggregationPolicy sp = getTotalAggregationPolicy();
    if (totalAggregationPolicy.needCheckIntersection()) {
      Set<KeyTuple> keyTupleList;
      if (sp.isNotExtendAggregationPolicy()) {
        keyTupleList = getNormalResult();
      } else {
        if (CollectionUtils.isEmpty(this.totalConnectors)) {
          LOGGER.warn(
            "Total connector is empty while summary policy is SAME_AS_QUERY/SAME_AS_QUERY_EXTEND/QUERY. It is NOT valid in principle.");
          return;
        }
        keyTupleList = new HashSet<KeyTuple>();
        Map<Object, ResultTuple> m;

        Interval interval;
        String displayString;
        CommonFormulaQueryDescriptor cfqd;
        KeyTuple keyTuple;
        Object entryKey;
        for (FormulaQueryDescriptor fqd : this.totalConnectors) {
          if (KILL.equals(fqd.getDateTruncateType())) {
            continue;
          }
          m = safeGet(descriptorTupleMap, fqd);
          cfqd = (CommonFormulaQueryDescriptor) fqd;

          interval = cfqd.getInterval();
          displayString = getFullDisplayDateString(fqd.getInputBeginDate());

          if (MapUtils.isEmpty(m)) {
            keyTupleList.add(new KeyTuple(displayString, PENDING_RESULT_TUPLE, Pending.INSTANCE));
            continue;
          }
          cacheState = descriptorStateMap.get(fqd);
          status = getStatusFromCacheState(cacheState);

          for (Entry<Object, ResultTuple> entry : m.entrySet()) {
            entryKey = entry.getKey();
            if (interval.getDays() < 1) {
              keyTuple = new KeyTuple(entryKey, entryKey, entry.getValue(), status);
            } else {
              keyTuple = new KeyTuple(displayString, displayString, entry.getValue(), status);
            }
            keyTupleList.add(keyTuple);
          }
        }
      }
      boolean hasContent = CollectionUtils.isNotEmpty(totalKeyIntersection);

      ResultTuple accumulativeRT = null;
      int counterPending = 0;
      int counterNormal = 0;
      ResultTuple rt;
      for (KeyTuple kt : keyTupleList) {
        if (kt == null) {
          continue;
        }

        rt = kt.getResultTuple();
        if (rt == null) {
          continue;
        }
        status = kt.getStatus();

        if (Pending.isPendingPlaceholder(status)) {
          totalStatus = Pending.INSTANCE;
        }

        if (rt.isPendingPlaceholder()) {
          counterPending++;
        } else if (rt.isValidResultTuple() && hasContent && totalKeyIntersection.contains(kt.getAdditionalKey())) {
          counterNormal++;
          if (accumulativeRT == null) {
            accumulativeRT = createNewNullResultTuple();
          }
          accumulativeRT.incAll(rt);
        }
      }
      // 有正常的值, 就能计算出数据
      if (counterNormal > 0) {
        if (AVERAGE.equals(totalAggregationPolicy) || AVERAGE_EXTEND.equals(totalAggregationPolicy)) {
          accumulativeRT.doDivision(counterNormal);
        }
        setTotalAggregation(new KeyTuple(TOTAL, accumulativeRT, totalStatus));
      } else if (counterPending > 0) {
        setTotalAggregation(new KeyTuple(TOTAL, PENDING_RESULT_TUPLE, Pending.INSTANCE));
      } else {
        setTotalAggregation(new KeyTuple(TOTAL, NA_RESULT_TUPLE));
      }
    } else {
      if (SAME_AS_QUERY.equals(sp)) {
        for (KeyTuple kt : getNormalResult()) {
          setTotalAggregation(new KeyTuple(TOTAL, kt.getResultTuple(), kt.getStatus()));
        }
      } else {
        if (CollectionUtils.isEmpty(this.totalConnectors)) {
          LOGGER.warn(
            "Total connector is empty while summary policy is SAME_AS_QUERY/SAME_AS_QUERY_EXTEND/QUERY. It is NOT valid in principle.");
          return;
        }
        FormulaQueryDescriptor totalDescriptor = this.totalConnectors.get(0);
        if (totalDescriptor.isKilled()) {
          setTotalAggregation(new KeyTuple(TOTAL, NA_RESULT_TUPLE));
          return;
        }
        Map<Object, ResultTuple> map = safeGet(descriptorTupleMap, totalDescriptor);
        if (map == null) {
          setTotalAggregation(new KeyTuple(TOTAL, PENDING_RESULT_TUPLE, Pending.INSTANCE));
          return;
        }
        cacheState = descriptorStateMap.get(totalDescriptor);
        totalStatus = getStatusFromCacheState(cacheState);
        for (Entry<Object, ResultTuple> entry : map.entrySet()) {
          setTotalAggregation(new KeyTuple(TOTAL, entry.getValue(), totalStatus));
        }
      }
    }
  }

  /**
   * 生成Natural汇总, Natural汇总比Total汇总少了2个情况, 即Natural汇总没有ACCUMULATION_EXTEND和AVERAGE_EXTEND
   *
   * @param descriptorTupleMap
   */
  public void fillNaturalAggregation(Map<FormulaQueryDescriptor, Map<Object, ResultTuple>> descriptorTupleMap,
                                     Map<FormulaQueryDescriptor, CacheState> descriptorStateMap) throws
    DataFillingException {
    Object naturalStatus = null;
    Object status;
    CacheState cacheState;

    if (SAME_AS_TOTAL.equals(naturalAggregationPolicy)) {
      setNaturalAggregation(getTotalAggregation());
    } else if (naturalAggregationPolicy.needCheckIntersection()) {
      Set<KeyTuple> keyTupleList = getNormalResult();

      boolean hasContent = CollectionUtils.isNotEmpty(naturalKeyIntersection);

      ResultTuple accumulativeRT = null;
      int counterPending = 0;
      int counterNormal = 0;
      ResultTuple rt;
      for (KeyTuple kt : keyTupleList) {
        if (kt == null) {
          continue;
        }
        rt = kt.getResultTuple();
        status = kt.getStatus();

        if (Pending.isPendingPlaceholder(status)) {
          naturalStatus = Pending.INSTANCE;
        }
        if (rt == null) {
          continue;
        }
        if (rt.isPendingPlaceholder()) {
          counterPending++;
        } else if (rt.isValidResultTuple() && hasContent && naturalKeyIntersection.contains(kt.getAdditionalKey())) {
          counterNormal++;
          if (accumulativeRT == null) {
            accumulativeRT = createNewNullResultTuple();
          }
          accumulativeRT.incAll(rt);
        }
      }
      // 有正常的值, 就能计算出数据
      if (counterNormal > 0) {
        if (AVERAGE.equals(totalAggregationPolicy) || AVERAGE_EXTEND.equals(totalAggregationPolicy)) {
          accumulativeRT.doDivision(counterNormal);
        }
        setNaturalAggregation(new KeyTuple(NATURAL, accumulativeRT, naturalStatus));
      } else if (counterPending > 0) {
        setNaturalAggregation(new KeyTuple(NATURAL, PENDING_RESULT_TUPLE, Pending.INSTANCE));
      } else {
        setNaturalAggregation(new KeyTuple(NATURAL, NA_RESULT_TUPLE, NotAvailable.INSTANCE));
      }
    } else {
      if (SAME_AS_QUERY.equals(naturalAggregationPolicy)) {
        for (KeyTuple kt : getNormalResult()) {
          setNaturalAggregation(new KeyTuple(NATURAL, kt.getResultTuple(), kt.getStatus()));
        }
      } else {
        List<FormulaQueryDescriptor> naturalList = this.naturalConnectors;
        if (CollectionUtils.isEmpty(naturalList)) {
          LOGGER.warn(
            "Natural connector is empty while summary policy is SAME_AS_QUERY/SAME_AS_QUERY_EXTEND/QUERY. It is NOT valid in principle.");
          return;
        }
        FormulaQueryDescriptor naturalDescriptor = naturalList.get(0);

        if (KILL.equals(naturalDescriptor.getDateTruncateType())) {
          setNaturalAggregation(new KeyTuple(NATURAL, NA_RESULT_TUPLE, NotAvailable.INSTANCE));
          return;
        }
        Map<Object, ResultTuple> map = safeGet(descriptorTupleMap, naturalDescriptor);
        if (map == null) {
          setNaturalAggregation(new KeyTuple(NATURAL, PENDING_RESULT_TUPLE, Pending.INSTANCE));
          return;
        }
        cacheState = descriptorStateMap.get(naturalDescriptor);
        naturalStatus = getStatusFromCacheState(cacheState);
        for (Entry<Object, ResultTuple> entry : map.entrySet()) {
          setNaturalAggregation(new KeyTuple(NATURAL, entry.getValue(), naturalStatus));
          break;
        }
      }
    }
  }

  @Override
  public Collection<FormulaQueryDescriptor> distinctDescriptor() throws NecessaryCollectionEmptyException {
    boolean emptyNormal = this.normalConnectors == null;
    boolean emptyTotal = this.totalConnectors == null;
    boolean emptyNatural = this.naturalConnectors == null;

    int size = emptyNormal ? 0 : this.normalConnectors.size();
    size += emptyTotal ? 0 : this.totalConnectors.size();
    size += emptyNatural ? 0 : this.normalConnectors.size();

    List<FormulaQueryDescriptor> descriptors = new ArrayList<FormulaQueryDescriptor>(size);
    if (!emptyNormal) {
      descriptors.addAll(normalConnectors);
    }
    if (!emptyTotal) {
      descriptors.addAll(totalConnectors);
    }
    if (!emptyNatural) {
      descriptors.addAll(naturalConnectors);
    }
    return distinctFromCollection(descriptors);
  }

}
