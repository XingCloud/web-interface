package com.xingcloud.webinterface.model.intermediate;

import static com.xingcloud.webinterface.model.aggregation.Accumulator.accumulateStrictly;

import com.xingcloud.webinterface.calculate.ScaleGroup;
import com.xingcloud.webinterface.enums.AggregationPolicy;
import com.xingcloud.webinterface.enums.CacheState;
import com.xingcloud.webinterface.exception.DataFillingException;
import com.xingcloud.webinterface.exception.NecessaryCollectionEmptyException;
import com.xingcloud.webinterface.model.ResultTuple;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 代理层, 代理层的作用是将具有类似segment的ItemResult汇总到一起, 将结果加总(包括普通结果中的时间线, 汇总, 以及各个GroupBy结果)返还给IdResult. 之所以可以加总, 是认为同一个用户在该时间段内,
 * 只可能具有一个状态, 如服id, 总体而言, 代理层存在的目的, 是为了处理segment中的in操作, 如 "segment":"{'identifier':['a','b','c']}" 该segment的值=
 * "segment":"{'identifier':'a'}" + "segment":"{'identifier':'b'}" + "segment":"{'identifier':'c'}"
 * <p/>
 * 一个Proxy具有的最基本的属性是一组相似segment的itemResult的列表, 具有的最基本的功能是汇总这些列表里的结果
 *
 * @author Z J Wu@2013-1-16
 */
public class CommonItemResultGroup extends CommonItemResult {

  private Collection<CommonItemResult> commonItemResults;

  public CommonItemResultGroup(String name, ScaleGroup scaleGroup, AggregationPolicy totalAggregationPolicy,
                               AggregationPolicy naturalAggregationPolicy,
                               Collection<CommonItemResult> commonItemResults) {
    super(name, scaleGroup, totalAggregationPolicy, naturalAggregationPolicy);
    this.commonItemResults = commonItemResults;
  }

  @Override
  public void fillNormalResult(Map<FormulaQueryDescriptor, Map<Object, ResultTuple>> descriptorTupleMap,
                               Map<FormulaQueryDescriptor, CacheState> descriptorStateMap) throws DataFillingException {
    Set<KeyTuple> tmpKTSet;

    Map<Object, KeyTuple> keyTupleMap = null;
    KeyTuple existsKeyTuple, newKt;
    // 这是Common查询, 预先可以知道所有的Key是什么, 因此无需事先制作Key的并集
    Object k;
    for (CommonItemResult cir : commonItemResults) {
      cir.fillNormalResult(descriptorTupleMap, descriptorStateMap);
      tmpKTSet = cir.getNormalResult();
      if (CollectionUtils.isEmpty(tmpKTSet)) {
        continue;
      }
      if (keyTupleMap == null) {
        keyTupleMap = new HashMap<Object, KeyTuple>(tmpKTSet.size());
      }
      for (KeyTuple kt : tmpKTSet) {
        k = kt.getKey();
        existsKeyTuple = keyTupleMap.get(k);
        newKt = accumulateStrictly(existsKeyTuple, kt);
        keyTupleMap.put(k, newKt);
      }
    }

    Set<KeyTuple> thisKeyTupleSet = getNormalResult();
    if (thisKeyTupleSet == null) {
      thisKeyTupleSet = new HashSet<KeyTuple>();
      setNormalResult(thisKeyTupleSet);
    }
    if (CollectionUtils.isNotEmpty(thisKeyTupleSet)) {
      thisKeyTupleSet.clear();
    }
    if (MapUtils.isNotEmpty(keyTupleMap)) {
      thisKeyTupleSet.addAll(keyTupleMap.values());
    }
  }

  @Override
  public void fillTotalAggregation(Map<FormulaQueryDescriptor, Map<Object, ResultTuple>> descriptorTupleMap,
                                   Map<FormulaQueryDescriptor, CacheState> descriptorStateMap) throws
    DataFillingException {

    KeyTuple totalKT = null;
    for (CommonItemResult cir : commonItemResults) {
      cir.fillTotalAggregation(descriptorTupleMap, descriptorStateMap);
      totalKT = cir.getTotalAggregation();
      break;
    }
    setTotalAggregation(totalKT);
  }

  @Override
  public void fillNaturalAggregation(Map<FormulaQueryDescriptor, Map<Object, ResultTuple>> descriptorTupleMap,
                                     Map<FormulaQueryDescriptor, CacheState> descriptorStateMap) throws
    DataFillingException {

    KeyTuple naturalKT;
    KeyTuple existsNaturalKT = null;

    for (CommonItemResult cir : commonItemResults) {
      cir.fillNaturalAggregation(descriptorTupleMap, descriptorStateMap);
      naturalKT = cir.getNaturalAggregation();
      existsNaturalKT = accumulateStrictly(existsNaturalKT, naturalKT);
    }

    setNaturalAggregation(existsNaturalKT);
  }

  @Override
  public Collection<FormulaQueryDescriptor> distinctDescriptor() throws NecessaryCollectionEmptyException {
    if (CollectionUtils.isEmpty(commonItemResults)) {
      return null;
    }
    Collection<FormulaQueryDescriptor> subDescriptors;
    Collection<Collection<FormulaQueryDescriptor>> descriptors = new ArrayList<Collection<FormulaQueryDescriptor>>(
      this.commonItemResults.size());
    for (ItemResult gbir : commonItemResults) {
      subDescriptors = gbir.distinctDescriptor();
      if (CollectionUtils.isEmpty(subDescriptors)) {
        continue;
      }
      descriptors.add(subDescriptors);
    }
    return distinctFromCollections(descriptors);
  }

  public Collection<CommonItemResult> getCommonItemResults() {
    return commonItemResults;
  }

}
