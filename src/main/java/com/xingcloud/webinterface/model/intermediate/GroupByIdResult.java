package com.xingcloud.webinterface.model.intermediate;

import static com.xingcloud.basic.Constants.INTERNAL_NA;
import static com.xingcloud.basic.utils.DateUtils.isValidShortDate;
import static com.xingcloud.basic.utils.DateUtils.short2Date;
import static com.xingcloud.webinterface.enums.SliceType.DATED;
import static com.xingcloud.webinterface.enums.SliceType.NUMERIC;
import static com.xingcloud.webinterface.model.NotAvailable.isNotAvailablePlaceholder;
import static com.xingcloud.webinterface.model.Pending.isPendingPlaceholder;
import static com.xingcloud.webinterface.model.ResultTuple.NA_RESULT_TUPLE;
import static com.xingcloud.webinterface.model.ResultTuple.PENDING_RESULT_TUPLE;
import static com.xingcloud.webinterface.model.ResultTuple.createNewNullResultTuple;
import static com.xingcloud.webinterface.utils.IntermediateResultUtils.spreadStatus;
import static com.xingcloud.webinterface.utils.WebInterfaceRandomUtils.randomTuple;
import static com.xingcloud.webinterface.utils.range.RangeUtils.getXRangeFromDate;
import static com.xingcloud.webinterface.utils.range.RangeUtils.getXRangeFromLong;
import static com.xingcloud.webinterface.utils.range.XRange.INTERNAL_NA_LONG_XRANGE;
import static org.apache.commons.lang.StringUtils.isNumeric;

import com.xingcloud.webinterface.calculate.Arity;
import com.xingcloud.webinterface.calculate.Evaluator;
import com.xingcloud.webinterface.enums.CacheState;
import com.xingcloud.webinterface.enums.Function;
import com.xingcloud.webinterface.enums.SliceType;
import com.xingcloud.webinterface.exception.DataFillingException;
import com.xingcloud.webinterface.exception.NecessaryCollectionEmptyException;
import com.xingcloud.webinterface.exception.NumberOfDayException;
import com.xingcloud.webinterface.exception.RangingException;
import com.xingcloud.webinterface.exception.SegmentException;
import com.xingcloud.webinterface.exception.XParameterException;
import com.xingcloud.webinterface.model.NotAvailable;
import com.xingcloud.webinterface.model.NotAvailableNumber;
import com.xingcloud.webinterface.model.Pending;
import com.xingcloud.webinterface.model.PendingNumber;
import com.xingcloud.webinterface.model.ResultTuple;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import com.xingcloud.webinterface.utils.range.XRange;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

public class GroupByIdResult extends IdResult {
  private static final Logger LOGGER = Logger.getLogger(GroupByIdResult.class);

  private String slicePattern;

  private SliceType sliceType;

  // 附加描述, 描述本Groupby的查询时间
  private String description;

  private Map<String, GroupByItemResult> itemResultMap;

  // 聚合计算-统一化聚合口径-killed的fqd检查
  private Set<Object> killedFqdSet;
  // 聚合计算-统一化聚合口径-passed的fqd检查
  private Set<Object> missedFqdSet;

  public GroupByIdResult(String id, String formula, Map<String, Function> functionMap) {
    super(id, formula, functionMap);
  }

  public GroupByIdResult(String id, String formula, Map<String, Function> functionMap,
                         Map<String, GroupByItemResult> itemResultMap) {
    super(id, formula, functionMap);
    this.itemResultMap = itemResultMap;
  }

  public GroupByIdResult(String id, String formula, Map<String, Function> functionMap, String slicePattern,
                         SliceType sliceType, Map<String, GroupByItemResult> itemResultMap) {
    super(id, formula, functionMap);
    this.slicePattern = slicePattern;
    this.sliceType = sliceType;
    this.itemResultMap = itemResultMap;
  }

  public GroupByIdResult(String id, String formula, Map<String, Function> functionMap, String slicePattern,
                         SliceType sliceType, String description, Map<String, GroupByItemResult> itemResultMap,
                         Set<Object> killedFqdSet, Set<Object> missedFqdSet) {
    super(id, formula, functionMap);
    this.slicePattern = slicePattern;
    this.sliceType = sliceType;
    this.description = description;
    this.itemResultMap = itemResultMap;
    this.killedFqdSet = killedFqdSet;
    this.missedFqdSet = missedFqdSet;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getSlicePattern() {
    return slicePattern;
  }

  public void setSlicePattern(String slicePattern) {
    this.slicePattern = slicePattern;
  }

  public Map<String, GroupByItemResult> getItemResultMap() {
    return itemResultMap;
  }

  public void setItemResultMap(Map<String, GroupByItemResult> itemResultMap) {
    this.itemResultMap = itemResultMap;
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

  public void integrateResult(Map<FormulaQueryDescriptor, Map<Object, ResultTuple>> descriptorTupleMap,
                              Map<FormulaQueryDescriptor, CacheState> descriptorStateMap) throws DataFillingException,
    RangingException, NecessaryCollectionEmptyException {
    String name;
    GroupByItemResult gbir;

    Map<Object, Map<String, ResultTuple>> outerFlippedMap = new HashMap<Object, Map<String, ResultTuple>>();
    Map<String, ResultTuple> innerFlippedMap;

    Set<KeyTuple> ktList;
    boolean needRanging = StringUtils.isNotBlank(slicePattern);
    Collection<XRange<Long>> ranges = null;
    if (needRanging) {
      if (NUMERIC.equals(sliceType)) {
        ranges = getXRangeFromLong(slicePattern, sliceType);
      } else {
        ranges = getXRangeFromDate(slicePattern, sliceType);
      }
    }
    Object k;
    ResultTuple v;
    Object status = null;

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("[GROUPBY-ID-RESULT] - [" + id + "] - Querying date - " + description);
      LOGGER.debug("[GROUPBY-ID-RESULT] - [" + id + "] - Normalize if necessary.");
    }
    for (Entry<String, GroupByItemResult> entry : itemResultMap.entrySet()) {
      gbir = entry.getValue();
      gbir.filterKilledFqd(descriptorTupleMap);
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("[GROUPBY-ID-RESULT] - [" + id + "] - Killed set - " + new TreeSet<Object>(getKilledFqdSet()));
    }
    for (Entry<String, GroupByItemResult> entry : itemResultMap.entrySet()) {
      gbir = entry.getValue();
      gbir.filterMissedFqd(descriptorTupleMap);
    }

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("[GROUPBY-ID-RESULT] - [" + id + "] - Cache-missed set - " + new TreeSet<Object>(getMissedFqdSet()));
    }
    Set<String> names = new HashSet<String>(itemResultMap.size());
    for (Entry<String, GroupByItemResult> entry : itemResultMap.entrySet()) {
      name = entry.getKey();
      gbir = entry.getValue();

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("[GROUPBY-ID-RESULT] - [" + id + ", " + name + "] - Fill Normal data.");
      }
      gbir.fillNormalResult(descriptorTupleMap, descriptorStateMap);

      status = spreadStatus(status, gbir.getStatus());

      ktList = gbir.getNormalResult();
      if (CollectionUtils.isEmpty(ktList)) {
        LOGGER.warn(
          "[GROUPBY-ID-RESULT] - [" + id + ", " + name + "] - has no any data. Status is [" + gbir.getStatus() + "]");
        continue;
      }
      names.add(name);
      if (needRanging) {
        ktList = range(ranges, ktList, sliceType);
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("[GROUPBY-ID-RESULT] - [" + id + ", " + name + "] - Ranging finished.");
        }
      }
      for (KeyTuple kt : ktList) {
        k = kt.getKey();
        innerFlippedMap = outerFlippedMap.get(k);
        if (innerFlippedMap == null) {
          innerFlippedMap = new HashMap<String, ResultTuple>();
          outerFlippedMap.put(k, innerFlippedMap);
        }
        v = kt.getResultTuple();
        innerFlippedMap.put(name, v);
      }
    }

    fill0(outerFlippedMap, names);
    setInputData(outerFlippedMap);
    setStatus(status);
  }

  private Set<KeyTuple> range(Collection<XRange<Long>> ranges, Set<KeyTuple> ktList, SliceType sliceType) throws
    RangingException {
    if (CollectionUtils.isEmpty(ranges)) {
      return ktList;
    }
    if (CollectionUtils.isEmpty(ktList)) {
      return null;
    }
    Set<KeyTuple> rangedKtList = new HashSet<KeyTuple>(ranges.size());
    Map<XRange<Long>, ResultTuple> rangeMap = new HashMap<XRange<Long>, ResultTuple>(ranges.size());
    Object k;
    ResultTuple v, internalNAResultTuple = null;
    String keyString;
    Long keyLong;
    ResultTuple existsTuple;

    for (KeyTuple kt : ktList) {
      k = kt.getKey();
      if (isNotAvailablePlaceholder(k)) {
        LOGGER.warn("[RANGING] - Ignore ranging group by V - " + NotAvailable.INSTANCE);
        rangedKtList.add(kt);
        continue;
      }
      if (isPendingPlaceholder(k)) {
        LOGGER.warn("[RANGING] - Ignore ranging group by V - " + Pending.INSTANCE);
        rangedKtList.add(kt);
        continue;
      }
      keyString = k.toString();
      v = kt.getResultTuple();
      // 内部NA的直接过
      if (INTERNAL_NA.equals(keyString)) {
        if (internalNAResultTuple == null) {
          internalNAResultTuple = createNewNullResultTuple();
        }
        internalNAResultTuple.incAll(v);
        continue;
      }

      if (NUMERIC.equals(sliceType) && isNumeric(keyString)) {
        keyLong = Long.parseLong(keyString);
      } else if (DATED.equals(sliceType) && isValidShortDate(keyString)) {
        try {
          keyLong = short2Date(keyString).getTime();
        } catch (ParseException e) {
          throw new RangingException("Cannot convert date string to long - " + keyString, e);
        }
      } else {
        throw new RangingException("Unsupported ranging pattern - " + keyString);
      }

      for (XRange<Long> xRange : ranges) {
        if (xRange.contains(keyLong)) {
          existsTuple = rangeMap.get(xRange);
          if (existsTuple == null) {
            existsTuple = createNewNullResultTuple();
            rangeMap.put(xRange, existsTuple);
          }
          existsTuple.incAll(v);
          break;
        }
      }
    }
    if (internalNAResultTuple != null) {
      rangeMap.put(INTERNAL_NA_LONG_XRANGE, internalNAResultTuple);
    }
    for (Entry<XRange<Long>, ResultTuple> entry : rangeMap.entrySet()) {
      rangedKtList.add(new KeyTuple(entry.getKey(), entry.getValue()));
    }
    return rangedKtList;
  }

  @Override
  public Map<Object, Number> calculate() {
    if (!check()) {
      return null;
    }
    Map<Object, Number> outputData = new HashMap<Object, Number>(inputData.size());
    Object key;
    Map<String, ResultTuple> valueMap;
    List<Arity> arity;
    String variable;
    ResultTuple rt;
    Number value;

    int size = inputData.size();

    boolean usingPendingDirectly = false, usingNADirectly = false;
    /*
     * 提前处理一些GroupBy才有的特殊情况, 并且给一些中间判断量赋值
     * 之所以先判断NotAvailable后判断Pending, 是因为NotAvailable拥有更高
     * 的结果导向性, 参看下表
     * GroupByKey=001, 002, P=Pending, N=NotAvailable, v=正常值
     *
     * (1). P+v: {GroupBy(001)=PendingNumber}
     * (2). P+N: {NotAvailablePlaceholder=NotAvailableNumber}
     * (3). N+v: {GroupBy(001)=NotAvailableNumber}
     * (4). N+N: {NotAvailablePlaceholder=NotAvailableNumber}
     * (5). P+P: {PendingPlaceholder=PendingNumber}
     * (6). v+v: {GroupBy(001)=7.9748, GroupBy(002)=1.209}
     *
     * 3种特殊情况可以直接返回:
     * a. 只有NotAvailable没别的, 就放一个值NotAvailable
     * b. 只有NotAvailable和Pending, 没别的, 就放一个值NotAvailable
     * c. 只有Pending, 没别的, 就放一个值Pending
     */
    if (inputData.containsKey(NotAvailable.INSTANCE)) {
      if (size == 1) {
        outputData.put(NotAvailable.INSTANCE, NotAvailableNumber.INSTANCE);
        return outputData;
      } else if (size == 2 && inputData.containsKey(Pending.INSTANCE)) {
        outputData.put(NotAvailable.INSTANCE, NotAvailableNumber.INSTANCE);
        return outputData;
      } else {
        usingNADirectly = true;
      }
    } else if (inputData.containsKey(Pending.INSTANCE)) {
      if (size == 1) {
        outputData.put(Pending.INSTANCE, PendingNumber.INSTANCE);
        return outputData;
      } else {
        usingPendingDirectly = true;
      }
    }

    if (usingNADirectly) {
      inputData.remove(NotAvailable.INSTANCE);
      inputData.remove(Pending.INSTANCE);
    } else if (usingPendingDirectly) {
      inputData.remove(Pending.INSTANCE);
    }

    String[] formulaInfo = parseFormula();
    String formula = formulaInfo[0];

    LOGGER.info("[CALCULATION] - GROUP, Formula(" + this.id + ") - " + formula);

    boolean hasNAResult, hasPendingResult;
    for (Entry<Object, Map<String, ResultTuple>> outerEntry : inputData.entrySet()) {
      key = outerEntry.getKey();

      if (usingNADirectly) {
        outputData.put(key, NotAvailableNumber.INSTANCE);
        continue;
      }
      if (usingPendingDirectly) {
        outputData.put(key, PendingNumber.INSTANCE);
        continue;
      }

      valueMap = outerEntry.getValue();
      if (MapUtils.isEmpty(valueMap)) {
        LOGGER.warn("[CALCULATION] - Key[" + id + ", " + key + "] will be ignored because it's V map is empty.");
        outputData.put(key, NotAvailableNumber.INSTANCE);
        continue;
      }

      arity = new ArrayList<Arity>(valueMap.size());
      hasNAResult = false;
      hasPendingResult = false;

      /*
       * Pending, NA, V(正常查询结果)的组合结果
       * 1. 只要出现NA, 不论其他是什么, 结果就是NA
       * 2. 全部是Value, 结果是Value
       * 3. 全部是PENDING, 结果是PENDING
       * 4. 既有PENDING, 也有Value, 结果是PENDING
       * 5. Value计算出错, 补NA
       */
      for (Entry<String, ResultTuple> innerEntry : valueMap.entrySet()) {
        variable = innerEntry.getKey();
        rt = innerEntry.getValue();
        if (rt == null || rt.isNAPlaceholder()) {
          hasNAResult = true;
          break;
        }

        if (rt.isPendingPlaceholder()) {
          hasPendingResult = true;
          continue;
        }
        value = rt.getEstimateValue(functionMap.get(variable));
        if (value != null) {
          arity.add(new Arity(variable, value.doubleValue()));
        }
      }
      if (hasNAResult) {
        value = NotAvailableNumber.INSTANCE;
      } else if (hasPendingResult) {
        value = PendingNumber.INSTANCE;
      } else {
        try {
          value = Evaluator.evaluateNumber(formula, arity);
        } catch (Exception e) {
//          LOGGER.error(e);
          value = NotAvailableNumber.INSTANCE;
        }
      }
      outputData.put(key, value);
    }
    return outputData;
  }

  @Override
  public Collection<FormulaQueryDescriptor> distinctDescriptor() throws NecessaryCollectionEmptyException {
    return distinctFromItemResultMap(itemResultMap);
  }

  public static void main(String[] args) throws XParameterException, NumberOfDayException, SegmentException {
    IdResult idr;
    String formula = "x/y";
    Map<String, Function> functionMap = new HashMap<String, Function>();
    functionMap.put("x", Function.COUNT);
    functionMap.put("y", Function.COUNT);

    // P+V
    Map<Object, Map<String, ResultTuple>> inputData1 = new HashMap<Object, Map<String, ResultTuple>>();
    Map<String, ResultTuple> m11 = new HashMap<String, ResultTuple>();
    m11.put("x", PENDING_RESULT_TUPLE);
    inputData1.put(Pending.INSTANCE, m11);

    Map<String, ResultTuple> m12 = new HashMap<String, ResultTuple>();
    m12.put("y", randomTuple());
    inputData1.put("GroupBy(001)", m12);

    idr = new GroupByIdResult("001", formula, functionMap);
    idr.setInputData(inputData1);
    System.out.println("1. P+V: " + idr.calculate());

    // P+N
    Map<Object, Map<String, ResultTuple>> inputData2 = new HashMap<Object, Map<String, ResultTuple>>();
    Map<String, ResultTuple> m21 = new HashMap<String, ResultTuple>();
    m21.put("x", PENDING_RESULT_TUPLE);
    inputData2.put(Pending.INSTANCE, m21);

    Map<String, ResultTuple> m22 = new HashMap<String, ResultTuple>();
    m22.put("y", NA_RESULT_TUPLE);
    inputData2.put(NotAvailable.INSTANCE, m22);

    idr = new GroupByIdResult("001", formula, functionMap);
    idr.setInputData(inputData2);
    System.out.println("2. P+N: " + idr.calculate());

    // N+V
    Map<Object, Map<String, ResultTuple>> inputData3 = new HashMap<Object, Map<String, ResultTuple>>();
    Map<String, ResultTuple> m31 = new HashMap<String, ResultTuple>();
    m31.put("x", NA_RESULT_TUPLE);
    inputData3.put(NotAvailable.INSTANCE, m31);

    Map<String, ResultTuple> m32 = new HashMap<String, ResultTuple>();
    m32.put("y", randomTuple());
    inputData3.put("GroupBy(001)", m32);

    idr = new GroupByIdResult("001", formula, functionMap);
    idr.setInputData(inputData3);
    System.out.println("3. N+V: " + idr.calculate());

    // N+N
    Map<Object, Map<String, ResultTuple>> inputData4 = new HashMap<Object, Map<String, ResultTuple>>();
    Map<String, ResultTuple> m41 = new HashMap<String, ResultTuple>();
    m41.put("x", NA_RESULT_TUPLE);
    m41.put("y", NA_RESULT_TUPLE);
    inputData4.put(NotAvailable.INSTANCE, m41);

    idr = new GroupByIdResult("001", formula, functionMap);
    idr.setInputData(inputData4);
    System.out.println("4. N+N: " + idr.calculate());

    // P+P
    Map<Object, Map<String, ResultTuple>> inputData5 = new HashMap<Object, Map<String, ResultTuple>>();
    Map<String, ResultTuple> m51 = new HashMap<String, ResultTuple>();
    m51.put("x", PENDING_RESULT_TUPLE);
    m51.put("y", PENDING_RESULT_TUPLE);
    inputData5.put(Pending.INSTANCE, m51);

    idr = new GroupByIdResult("001", formula, functionMap);
    idr.setInputData(inputData5);
    System.out.println("5. P+P: " + idr.calculate());

    // V+V
    Map<Object, Map<String, ResultTuple>> inputData6 = new HashMap<Object, Map<String, ResultTuple>>();
    Map<String, ResultTuple> m61 = new HashMap<String, ResultTuple>();
    m61.put("x", randomTuple());
    m61.put("y", randomTuple());
    inputData6.put("GroupBy(001)", m61);
    Map<String, ResultTuple> m62 = new HashMap<String, ResultTuple>();
    m62.put("x", randomTuple());
    m62.put("y", randomTuple());
    inputData6.put("GroupBy(002)", m62);

    idr = new GroupByIdResult("001", formula, functionMap);
    idr.setInputData(inputData6);
    System.out.println("6. V+V: " + idr.calculate());
  }
}
