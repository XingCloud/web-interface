package com.xingcloud.webinterface.model.formula;

import com.xingcloud.webinterface.model.ResultTuple;

import java.util.Collection;
import java.util.Map;

public class CommonFormulaQueryDescriptorGroup extends CommonFormulaQueryDescriptor {

  private Collection<CommonFormulaQueryDescriptor> descriptors;

  private Object state;

  public CommonFormulaQueryDescriptorGroup(Collection<CommonFormulaQueryDescriptor> descriptors) {
    super();
    this.descriptors = descriptors;
  }

  public Collection<CommonFormulaQueryDescriptor> getDescriptors() {
    return descriptors;
  }

  public void setDescriptors(Collection<CommonFormulaQueryDescriptor> descriptors) {
    this.descriptors = descriptors;
  }

  public Object getState() {
    return state;
  }

  public Map<Object, ResultTuple> getTupleMap() {
    Map<Object, ResultTuple> tupleMap = null;

    return tupleMap;
  }

}
