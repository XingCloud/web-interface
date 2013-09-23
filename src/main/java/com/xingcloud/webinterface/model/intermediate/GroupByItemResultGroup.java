package com.xingcloud.webinterface.model.intermediate;

import static com.xingcloud.webinterface.model.aggregation.Accumulator.accumulateLoosely;
import static com.xingcloud.webinterface.utils.IntermediateResultUtils.spreadStatus;

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
public class GroupByItemResultGroup extends GroupByItemResult {

  private Collection<GroupByItemResult> groupByItemResults;

  public GroupByItemResultGroup(String name, Collection<GroupByItemResult> groupByItemResults) {
    super(name);
    this.groupByItemResults = groupByItemResults;
  }

  public GroupByItemResultGroup(String name) {
    super(name);
  }

  public Collection<GroupByItemResult> getGroupByItemResults() {
    return groupByItemResults;
  }

  public void setGroupByItemResults(Collection<GroupByItemResult> groupByItemResults) {
    this.groupByItemResults = groupByItemResults;
  }

  @Override
  public void filterKilledFqd(Map<FormulaQueryDescriptor, Map<Object, ResultTuple>> descriptorTupleMap) throws
    NecessaryCollectionEmptyException {
    if (CollectionUtils.isEmpty(this.groupByItemResults)) {
      throw new NecessaryCollectionEmptyException("GroupBy item results is null or empty.");
    }
    for (GroupByItemResult groupByItemResult : groupByItemResults) {
      groupByItemResult.filterKilledFqd(descriptorTupleMap);
    }
  }

  @Override
  public void filterMissedFqd(Map<FormulaQueryDescriptor, Map<Object, ResultTuple>> descriptorTupleMap) throws
    NecessaryCollectionEmptyException {
    if (CollectionUtils.isEmpty(this.groupByItemResults)) {
      throw new NecessaryCollectionEmptyException("GroupBy item results is null or empty.");
    }
    for (GroupByItemResult groupByItemResult : groupByItemResults) {
      groupByItemResult.filterMissedFqd(descriptorTupleMap);
    }
  }

  @Override
  public void fillNormalResult(Map<FormulaQueryDescriptor, Map<Object, ResultTuple>> descriptorTupleMap,
                               Map<FormulaQueryDescriptor, CacheState> descriptorStateMap) throws DataFillingException,
    NecessaryCollectionEmptyException {
    if (CollectionUtils.isEmpty(this.groupByItemResults)) {
      throw new NecessaryCollectionEmptyException("GroupBy item results is null or empty.");
    }
    Set<KeyTuple> tmpKTSet;

    Map<Object, KeyTuple> keyTupleMap = null;
    KeyTuple existsKeyTuple, newKt;
    // 这是Group查询, 无法预先得知所有的GroupBy项, 因此凡是有的就添加到集合
    Object k, status = null;

    for (GroupByItemResult gbir : groupByItemResults) {
      gbir.fillNormalResult(descriptorTupleMap, descriptorStateMap);
      tmpKTSet = gbir.getNormalResult();
      status = spreadStatus(status, gbir.getStatus());
      if (CollectionUtils.isEmpty(tmpKTSet)) {
        continue;
      }

      if (keyTupleMap == null) {
        keyTupleMap = new HashMap<Object, KeyTuple>(tmpKTSet.size());
      }
      for (KeyTuple kt : tmpKTSet) {
        k = kt.getKey();
        existsKeyTuple = keyTupleMap.get(k);
        newKt = accumulateLoosely(existsKeyTuple, kt);
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
    setStatus(status);
  }

  @Override
  public Collection<FormulaQueryDescriptor> distinctDescriptor() throws NecessaryCollectionEmptyException {
    if (CollectionUtils.isEmpty(groupByItemResults)) {
      return null;
    }
    Collection<FormulaQueryDescriptor> subDescriptors;
    Collection<Collection<FormulaQueryDescriptor>> descriptors = new ArrayList<Collection<FormulaQueryDescriptor>>(
      this.groupByItemResults.size());
    for (ItemResult gbir : groupByItemResults) {
      subDescriptors = gbir.distinctDescriptor();
      if (CollectionUtils.isEmpty(subDescriptors)) {
        continue;
      }
      descriptors.add(subDescriptors);
    }
    return distinctFromCollections(descriptors);
  }

}
