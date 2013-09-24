package com.xingcloud.webinterface.model.intermediate;

import static com.xingcloud.webinterface.enums.CommonQueryType.NATURAL;
import static com.xingcloud.webinterface.enums.CommonQueryType.TOTAL;
import static com.xingcloud.webinterface.utils.IntermediateResultUtils.spreadStatus;

import com.xingcloud.basic.utils.DateUtils;
import com.xingcloud.webinterface.calculate.Arity;
import com.xingcloud.webinterface.calculate.Evaluator;
import com.xingcloud.webinterface.enums.AggregationPolicyDisplayed;
import com.xingcloud.webinterface.enums.CacheState;
import com.xingcloud.webinterface.enums.Function;
import com.xingcloud.webinterface.exception.DataFillingException;
import com.xingcloud.webinterface.exception.NecessaryCollectionEmptyException;
import com.xingcloud.webinterface.model.NotAvailable;
import com.xingcloud.webinterface.model.NotAvailableNumber;
import com.xingcloud.webinterface.model.Pending;
import com.xingcloud.webinterface.model.PendingNumber;
import com.xingcloud.webinterface.model.ResultTuple;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class CommonIdResult extends IdResult {

  private static final Logger LOGGER = Logger.getLogger(CommonIdResult.class);

  private Map<String, CommonItemResult> itemResultMap;

  private Map<String, AggregationPolicyDisplayed> summaryPolicyTypeMap;

  public CommonIdResult(String id, String formula, Map<String, Function> functionMap) {
    super(id, formula, functionMap);
  }

  public CommonIdResult(String id, String formula, Map<String, Function> functionMap,
                        Map<String, CommonItemResult> itemResultMap) {
    super(id, formula, functionMap);
    this.itemResultMap = itemResultMap;
    init(itemResultMap.size());
  }

  public Map<String, CommonItemResult> getItemResultMap() {
    return itemResultMap;
  }

  public Map<String, AggregationPolicyDisplayed> getSummaryPolicyTypeMap() {
    return summaryPolicyTypeMap;
  }

  public void init(int containerSize) {
    CommonItemResult commonItemResult;
    AggregationPolicyDisplayed tapDisplayed;
    this.summaryPolicyTypeMap = new HashMap<String, AggregationPolicyDisplayed>(containerSize);
    for (Entry<String, CommonItemResult> entry : itemResultMap.entrySet()) {
      commonItemResult = entry.getValue();
      tapDisplayed = commonItemResult.getTotalAggregationPolicy().getDisplayName();
      this.summaryPolicyTypeMap.put(commonItemResult.getName(), tapDisplayed);
    }
  }

  public void integrateResult(Map<FormulaQueryDescriptor, Map<Object, ResultTuple>> descriptorTupleMap,
                              Map<FormulaQueryDescriptor, CacheState> descriptorStateMap) throws DataFillingException {
    String name;
    CommonItemResult cir;

    Map<Object, Map<String, ResultTuple>> outerFlippedMap = new HashMap<Object, Map<String, ResultTuple>>();
    Map<String, ResultTuple> innerFlippedMap;

    Set<KeyTuple> ktList;

    Object k, status = null, thisStatus = null;
    ResultTuple v;

    for (Entry<String, CommonItemResult> entry : itemResultMap.entrySet()) {
      name = entry.getKey();
      cir = entry.getValue();
      cir.fillNormalResult(descriptorTupleMap, descriptorStateMap);
      ktList = cir.getNormalResult();
      if (CollectionUtils.isEmpty(ktList)) {
        LOGGER.warn("[COMMON-ID-RESULT] - Item [" + name + "] has no any data.");
        continue;
      }
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("[COMMON-ID-RESULT] - [" + id + ", " + name + "] - Normal data is filled.");
      }
      for (KeyTuple kt : ktList) {
        k = kt.getKey();
        status = spreadStatus(kt.getStatus());
        innerFlippedMap = outerFlippedMap.get(k);
        if (status != null) {
          thisStatus = status;
        }
        if (innerFlippedMap == null) {
          innerFlippedMap = new HashMap<String, ResultTuple>();
          outerFlippedMap.put(k, innerFlippedMap);
        }
        v = kt.getResultTuple();
        innerFlippedMap.put(name, v);
      }
    }

    KeyTuple summary;
    Set<String> names = new HashSet<String>(itemResultMap.size());
    for (Entry<String, CommonItemResult> entry : itemResultMap.entrySet()) {
      name = entry.getKey();
      cir = entry.getValue();

      // Total汇总
      try {
        cir.fillTotalAggregation(descriptorTupleMap, descriptorStateMap);
      } catch (Exception e) {
        throw new DataFillingException("Failed to make total summary - " + e.getMessage(), e);
      }

      // Natural汇总
      try {
        cir.fillNaturalAggregation(descriptorTupleMap, descriptorStateMap);
      } catch (Exception e) {
        throw new DataFillingException("Failed to make natural summary - " + e.getMessage(), e);
      }

      names.add(name);

      innerFlippedMap = outerFlippedMap.get(TOTAL);
      if (innerFlippedMap == null) {
        innerFlippedMap = new HashMap<String, ResultTuple>();
        outerFlippedMap.put(TOTAL, innerFlippedMap);
      }
      summary = cir.getTotalAggregation();
      if (summary != null && summary.getResultTuple() != null) {
        innerFlippedMap.put(name, summary.getResultTuple());
        status = spreadStatus(summary.getStatus());
      }
      if (status != null) {
        thisStatus = status;
      }

      innerFlippedMap = outerFlippedMap.get(NATURAL);
      if (innerFlippedMap == null) {
        innerFlippedMap = new HashMap<String, ResultTuple>();
        outerFlippedMap.put(NATURAL, innerFlippedMap);
      }
      summary = cir.getNaturalAggregation();
      if (summary != null && summary.getResultTuple() != null) {
        innerFlippedMap.put(name, summary.getResultTuple());
        status = spreadStatus(summary.getStatus());
      }
      if (status != null) {
        thisStatus = status;
      }

      String totalString , naturalString ;
      if (cir.getTotalAggregation() != null) {
        ResultTuple totalRT = cir.getTotalAggregation().getResultTuple();
        if (totalRT.isNAPlaceholder()) {
          totalString = "(" + NotAvailable.INSTANCE.toString() + ")";
        } else if (totalRT.isPendingPlaceholder()) {
          totalString = Pending.INSTANCE.toString() + ")";
        } else {
          totalString = totalRT.toString();
        }
      } else {
        totalString = "(null)";
      }

      if (cir.getNaturalAggregation() != null) {
        ResultTuple naturalRT = cir.getNaturalAggregation().getResultTuple();
        if (naturalRT.isNAPlaceholder()) {
          naturalString = "(" + NotAvailable.INSTANCE.toString() + ")";
        } else if (naturalRT.isPendingPlaceholder()) {
          naturalString = "(" + Pending.INSTANCE.toString() + ")";
        } else {
          naturalString = naturalRT.toString();
        }
      } else {
        naturalString = "(null)";
      }

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
          "[COMMON-ITEM-RESULT] - [" + id + ", " + name + "] - Total summary" + totalString + " and natural summary" + naturalString + " is filled. Status is " + thisStatus);
      }
    }
    fill0(outerFlippedMap, names);
    setInputData(outerFlippedMap);
    setStatus(thisStatus);
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

    boolean hasNAResult, hasPendingResult;

    String[] formulaInfo = parseFormula();
    String formula = formulaInfo[0], unbendingFormula = formulaInfo[1], d = formulaInfo[2];
    boolean useUnbendingFormula = false;
    if (StringUtils.isNotBlank(unbendingFormula)) {
      useUnbendingFormula = true;
    }
//    LOGGER
//        .info("[CALCULATION] - COMMON, Formula(" + this.id + ") - " + formula);
//    LOGGER.info(
//        "[CALCULATION] - COMMON, Unbending formula(" + this.id + ") - " + unbendingFormula);
//    LOGGER.info("[CALCULATION] - COMMON, From date(" + this.id + ") - " + d);

    for (Entry<Object, Map<String, ResultTuple>> outerEntry : inputData.entrySet()) {
      key = outerEntry.getKey();
      valueMap = outerEntry.getValue();
      if (MapUtils.isEmpty(valueMap)) {
        LOGGER.warn("[CALCULATION] - Key[" + id + ", " + key + "] will be ignored because it's value map is empty.");
        outputData.put(key, null);
        continue;
      }

      arity = new ArrayList<Arity>(valueMap.size());
      hasNAResult = false;
      hasPendingResult = false;

      /*
       * Pending, NA, Value(正常查询结果)的组合结果
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
          if (!(TOTAL.equals(key) || NATURAL.equals(key)
          ) && useUnbendingFormula && DateUtils.before(key.toString(), d)) {
            value = Evaluator.evaluateNumber(unbendingFormula, arity);
          } else {
            value = Evaluator.evaluateNumber(formula, arity);
          }
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

  public static void main(String[] args) {
    String s = "(x*0.01)/(y*0.01)#x/y#2013-06-05";
    String[] s1 = StringUtils.split(s, '#');
    System.out.println(Arrays.toString(s1));
  }

}
