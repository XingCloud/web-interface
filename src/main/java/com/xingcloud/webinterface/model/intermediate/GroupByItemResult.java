package com.xingcloud.webinterface.model.intermediate;

import static com.xingcloud.webinterface.enums.AggregationPolicy.AVERAGE;
import static com.xingcloud.webinterface.model.Pending.isPendingPlaceholder;
import static com.xingcloud.webinterface.model.ResultTuple.NA_RESULT_TUPLE;
import static com.xingcloud.webinterface.model.ResultTuple.PENDING_RESULT_TUPLE;

import com.xingcloud.webinterface.calculate.ScaleGroup;
import com.xingcloud.webinterface.enums.AggregationPolicy;
import com.xingcloud.webinterface.enums.CacheState;
import com.xingcloud.webinterface.exception.DataFillingException;
import com.xingcloud.webinterface.exception.FormulaException;
import com.xingcloud.webinterface.exception.NecessaryCollectionEmptyException;
import com.xingcloud.webinterface.model.NotAvailable;
import com.xingcloud.webinterface.model.Pending;
import com.xingcloud.webinterface.model.ResultTuple;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class GroupByItemResult extends ItemResult {

  private static final Logger LOGGER = Logger.getLogger(GroupByItemResult.class);

  private List<FormulaQueryDescriptor> connector;

  private AggregationPolicy groupByAggregationPolicy;

  /*
   * 聚合策略
   *
   * 设公式f: x/y中的各Groupby由日期 d1, d2, d3, d4, d5 平均/加总得出
   * d1-d5的值v1, v2, v3, v4, v5有如下可能
   * p=正常Hit缓存, n=正常Miss缓存, k=kill descriptor
   *
   * key     d1  d2  d3  d4  d5
   * k1      p   p   p   k   k
   * k2      p   n   p   n   p
   *
   * 1. 做去掉Kill descriptor的交集运算, 此求交集, 不改变GroupBy结果的status
   * key     d1  d2  d3  d4  d5
   * k1      p   p   p
   * k2      p   n   p
   *
   * 2. 做去掉midded descriptor的交集运算, 此求交集, 改变GroupBy结果的status
   * key     d1  d2  d3  d4  d5
   * k1      p       p
   * k2      p       p
   *
   * 最终, 参与平均运算的实际Groupby是v1和v3
   *
   */
  // 聚合计算-统一化聚合口径-killed的fqd检查
  private Set<Object> killedFqdSet;
  // 聚合计算-统一化聚合口径-passed的fqd检查
  private Set<Object> missedFqdSet;

  public GroupByItemResult(String name, ScaleGroup scaleGroup) {
    super(name, scaleGroup);
  }

  public GroupByItemResult(String name, ScaleGroup scaleGroup, List<FormulaQueryDescriptor> connector,
                           AggregationPolicy groupByAggregationPolicy, Set<Object> killedFqdSet,
                           Set<Object> missedFqdSet) {
    super(name, scaleGroup);
    this.connector = connector;
    this.groupByAggregationPolicy = groupByAggregationPolicy;
    this.killedFqdSet = killedFqdSet;
    this.missedFqdSet = missedFqdSet;
  }

  public List<FormulaQueryDescriptor> getConnector() {
    return connector;
  }

  public void setConnector(List<FormulaQueryDescriptor> connector) {
    this.connector = connector;
  }

  public AggregationPolicy getGroupByAggregationPolicy() {
    return groupByAggregationPolicy;
  }

  public void setGroupByAggregationPolicy(AggregationPolicy groupByAggregationPolicy) {
    this.groupByAggregationPolicy = groupByAggregationPolicy;
  }

  public void filterKilledFqd(Map<FormulaQueryDescriptor, Map<Object, ResultTuple>> descriptorTupleMap) throws
    NecessaryCollectionEmptyException {
    if (CollectionUtils.isEmpty(connector)) {
      throw new NecessaryCollectionEmptyException("Connector is null or empty.");
    }

    if (!this.groupByAggregationPolicy.needCheckIntersection()) {
      return;
    }

    String displayDate;
    for (FormulaQueryDescriptor fqd : connector) {
      displayDate = fqd.getInputBeginDate();
      if (fqd.isKilled()) {
        this.killedFqdSet.add(displayDate);
        continue;
      }
    }
  }

  public void filterMissedFqd(Map<FormulaQueryDescriptor, Map<Object, ResultTuple>> descriptorTupleMap) throws
    NecessaryCollectionEmptyException {
    if (CollectionUtils.isEmpty(connector)) {
      throw new NecessaryCollectionEmptyException("Connector is null or empty.");
    }

    if (!this.groupByAggregationPolicy.needCheckIntersection()) {
      return;
    }

    Map<Object, ResultTuple> tupleMap;

    String displayDate;
    for (FormulaQueryDescriptor fqd : connector) {
      displayDate = fqd.getInputBeginDate();
      // 如果已经在kill里了, 就不要重复检查了
      if (this.killedFqdSet.contains(displayDate)) {
        continue;
      }

      tupleMap = descriptorTupleMap.get(fqd);
      if (tupleMap == null) {
        this.missedFqdSet.add(displayDate);
        continue;
      }
    }
  }

  void fillNormalResult(Map<FormulaQueryDescriptor, Map<Object, ResultTuple>> descriptorTupleMap,
                        Map<FormulaQueryDescriptor, CacheState> descriptorStateMap) throws DataFillingException,
    NecessaryCollectionEmptyException {

    boolean allKilled = true;
    List<Map<Object, ResultTuple>> dataList = null;
    Map<Object, ResultTuple> tupleMap, tupleMapDuplicated = null;

    String inputDate;
    boolean needCheckIntersection = this.groupByAggregationPolicy.needCheckIntersection();
    Object genericStatus = null;
    double scaleRate;
    for (FormulaQueryDescriptor fqd : connector) {
      if (fqd.isKilled()) {
        LOGGER.info("[GROUP-BY-ITEM-RESULT] - " + fqd.getKey() + " ignored, killed.");
        continue;
      }

      if (needCheckIntersection) {
        inputDate = fqd.getInputBeginDate();

        // 先检查Kill
        if (this.killedFqdSet.contains(inputDate)) {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("[GROUP-BY-ITEM-RESULT] - " + fqd.getKey() + " ignored, killed by other.");
          }
          continue;
        }
        // 在检查missed-cache
        if (this.missedFqdSet.contains(inputDate)) {
          if (isPendingPlaceholder(genericStatus)) {
            if (LOGGER.isDebugEnabled()) {
              LOGGER
                .debug("[GROUP-BY-ITEM-RESULT] - " + fqd.getKey() + " ignored, missed-cache, keep status as Pending.");
            }
          } else {
            genericStatus = Pending.INSTANCE;
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug(
                "[GROUP-BY-ITEM-RESULT] - " + fqd.getKey() + " ignored, missed-cache, change status to Pending.");
            }
          }
          continue;
        }
      }

      allKilled = false;
      tupleMap = descriptorTupleMap.get(fqd);
      try {
        scaleRate = scaleGroup.getScale(fqd.getRealBeginDate());
      } catch (FormulaException e) {
        throw new DataFillingException(e);
      }
      if (tupleMap == null) {
        if (isPendingPlaceholder(genericStatus)) {
          if (LOGGER.isDebugEnabled()) {
            LOGGER
              .debug("[GROUP-BY-ITEM-RESULT] - " + fqd.getKey() + " ignored, missed-cache, keep status as Pending.");
          }
        } else {
          genericStatus = Pending.INSTANCE;
          if (LOGGER.isDebugEnabled()) {
            LOGGER
              .debug("[GROUP-BY-ITEM-RESULT] - " + fqd.getKey() + " ignored, missed-cache, change status to Pending.");
          }
        }
        continue;
      }
      ResultTuple rt;
      tupleMapDuplicated = new HashMap<Object, ResultTuple>(tupleMap.size());
      for (Entry<Object, ResultTuple> entry : tupleMap.entrySet()) {
        rt = entry.getValue().duplicate();
        rt.expandOrContract(scaleRate);
        tupleMapDuplicated.put(entry.getKey(), rt);
      }
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("[GROUP-BY-ITEM-RESULT] - " + fqd.getKey() + " added to AG-LIST.");
      }
      if (dataList == null) {
        dataList = new ArrayList<Map<Object, ResultTuple>>();
      }
      dataList.add(tupleMapDuplicated);
    }

    tupleMapDuplicated = doAggregation(dataList, this.groupByAggregationPolicy);

    Set<KeyTuple> ktList;
    KeyTuple kt;
    if (tupleMapDuplicated == null) {
      ktList = new HashSet<KeyTuple>(1);
      kt = allKilled ? new KeyTuple(NotAvailable.INSTANCE, NA_RESULT_TUPLE)
                     : new KeyTuple(Pending.INSTANCE, PENDING_RESULT_TUPLE);
      ktList.add(kt);
    } else {
      ktList = new HashSet<KeyTuple>(tupleMapDuplicated.size());
      for (Entry<Object, ResultTuple> entry : tupleMapDuplicated.entrySet()) {
        ktList.add(new KeyTuple(entry.getKey(), entry.getValue()));
      }
    }
    setNormalResult(ktList);
    setStatus(genericStatus);
  }

  private Map<Object, ResultTuple> doAggregation(List<Map<Object, ResultTuple>> data, AggregationPolicy ap) {
    if (data == null) {
      return null;
    }
    Map<Object, ResultTuple> result = new HashMap<Object, ResultTuple>();
    Object k;
    ResultTuple rt, existsRT;

    for (Map<Object, ResultTuple> rtMap : data) {
      if (rtMap.isEmpty()) {
        continue;
      }
      for (Entry<Object, ResultTuple> entry : rtMap.entrySet()) {
        k = entry.getKey();
        rt = entry.getValue();
        if (k == null || rt == null) {
          continue;
        }
        existsRT = result.get(k);
        if (existsRT == null) {
          result.put(k, rt.duplicate());
          continue;
        }
        existsRT.incAll(rt);
      }
    }

    if (AVERAGE.equals(ap)) {
      for (ResultTuple resultTuple : result.values()) {
        resultTuple.doDivision(data.size());
      }
    }

    return result;
  }

  public Collection<FormulaQueryDescriptor> distinctDescriptor() throws NecessaryCollectionEmptyException {
    if (CollectionUtils.isEmpty(connector)) {
      throw new NecessaryCollectionEmptyException(
        "A groupby item result must has at least one descriptor in connector, actural is " + connector);
    }
    return distinctFromCollection(connector);
  }

  public Set<Object> getKilledFqdSet() {
    return killedFqdSet;
  }

  public void setKilledFqdSet(Set<Object> killedFqdSet) {
    this.killedFqdSet = killedFqdSet;
  }

  public Set<Object> getMissedFqdSet() {
    return missedFqdSet;
  }

  public void setMissedFqdSet(Set<Object> missedFqdSet) {
    this.missedFqdSet = missedFqdSet;
  }

}
