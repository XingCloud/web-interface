package com.xingcloud.webinterface.model.intermediate;

import com.xingcloud.webinterface.enums.CacheState;
import com.xingcloud.webinterface.exception.DataFillingException;
import com.xingcloud.webinterface.exception.NecessaryCollectionEmptyException;
import com.xingcloud.webinterface.model.ResultTuple;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;

import java.util.Map;
import java.util.Set;

public abstract class ItemResult extends DescriptorDistinctor {

  protected String name;

  protected Set<KeyTuple> normalResult;

  protected Object status;

  public ItemResult(String name) {
    super();
    this.name = name;
  }

  abstract void fillNormalResult(Map<FormulaQueryDescriptor, Map<Object, ResultTuple>> descriptorTupleMap,
                                 Map<FormulaQueryDescriptor, CacheState> descriptorStateMap) throws
    DataFillingException, NecessaryCollectionEmptyException;

  public Object getStatus() {
    return status;
  }

  public void setStatus(Object status) {
    this.status = status;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Set<KeyTuple> getNormalResult() {
    return normalResult;
  }

  public void setNormalResult(Set<KeyTuple> normalResult) {
    this.normalResult = normalResult;
  }
}
