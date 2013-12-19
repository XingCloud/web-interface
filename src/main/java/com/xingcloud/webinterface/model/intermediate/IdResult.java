package com.xingcloud.webinterface.model.intermediate;

import static com.xingcloud.webinterface.model.NotAvailable.isNotAvailablePlaceholder;
import static com.xingcloud.webinterface.model.Pending.isPendingPlaceholder;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.FORMULA_ARITY_X;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.FORMULA_ARITY_Y;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.V9_SPLITTOR;

import com.xingcloud.webinterface.calculate.ScaleGroup;
import com.xingcloud.webinterface.enums.CacheState;
import com.xingcloud.webinterface.enums.Function;
import com.xingcloud.webinterface.enums.MathOperation;
import com.xingcloud.webinterface.exception.DataFillingException;
import com.xingcloud.webinterface.exception.FormulaException;
import com.xingcloud.webinterface.exception.NecessaryCollectionEmptyException;
import com.xingcloud.webinterface.exception.RangingException;
import com.xingcloud.webinterface.model.ResultTuple;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.Set;

public abstract class IdResult extends DescriptorDistinctor {

  private static final Logger LOGGER = Logger.getLogger(IdResult.class);

  protected String id;

  protected String formula;

  protected MathOperation mathOperation;

  protected Map<String, Function> functionMap;

  protected Map<Object, Map<String, ResultTuple>> inputData;

  protected Object status;

  public IdResult(String id, String formula, Map<String, Function> functionMap) {
    super();
    this.id = id;
    this.formula = formula;
    this.functionMap = functionMap;
  }

  public IdResult(String id, MathOperation mathOperation, Map<String, Function> functionMap) {
    super();
    this.id = id;
    this.mathOperation = mathOperation;
    this.functionMap = functionMap;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public Map<Object, Map<String, ResultTuple>> getInputData() {
    return inputData;
  }

  public void setInputData(Map<Object, Map<String, ResultTuple>> inputData) {
    this.inputData = inputData;
  }

  public Object getStatus() {
    return status;
  }

  public void setStatus(Object status) {
    this.status = status;
  }

  public abstract Map<Object, Number> calculate() throws FormulaException;

  public abstract void integrateResult(Map<FormulaQueryDescriptor, Map<Object, ResultTuple>> descriptorTupleMap,
                                       Map<FormulaQueryDescriptor, CacheState> descriptorStateMap) throws
    DataFillingException, RangingException, NecessaryCollectionEmptyException;

  public boolean check() {
//    if (Strings.isNullOrEmpty(formula)) {
//      LOGGER.warn("[ID-RESULT] - There is no formula for [" + id + "] to calculate.");
//      return false;
//    }
    if (MapUtils.isEmpty(functionMap)) {
      LOGGER.warn("[ID-RESULT] - There is no function map for [" + id + "] to calculate.");
      return false;
    }
    if (MapUtils.isEmpty(inputData)) {
      LOGGER.warn("[ID-RESULT] - There is no any input for [" + id + "] to calculate.");
      return false;
    }
    return true;
  }

  protected void fill0(Map<Object, Map<String, ResultTuple>> map, Set<String> names) {
    Object k;
    Map<String, ResultTuple> vm;
    for (Map.Entry<Object, Map<String, ResultTuple>> outerEntry : map.entrySet()) {
      k = outerEntry.getKey();
      if (isPendingPlaceholder(k) || isNotAvailablePlaceholder(k)) {
        continue;
      }
      vm = outerEntry.getValue();
      for (String n : names) {
        if (vm.containsKey(n)) {
          continue;
        }
        vm.put(n, ResultTuple.createNewEmptyResultTuple());
      }
    }
  }

  protected String parseFormula(ScaleGroup sg1, ScaleGroup sg2, MathOperation operation, String d,
                                boolean oneArity) throws FormulaException {
    StringBuilder sb = new StringBuilder();
    if (oneArity) {
      sb.append(FORMULA_ARITY_X);
      sb.append('*');
      sb.append(sg1.getScale(d));
      return sb.toString();
    }
    String expr = operation.getExpr();
    sb.append("(x*");
    sb.append(sg1.getScale(d));
    sb.append(')');
    sb.append(expr);
    sb.append("(y*");
    sb.append(sg2.getScale(d));
    sb.append(')');
    return sb.toString();
  }

  protected String parseFormula(MathOperation operation, boolean oneArity) throws FormulaException {
    if (oneArity) {
      return FORMULA_ARITY_X;
    }
    StringBuilder sb = new StringBuilder();
    String expr = operation.getExpr();
    sb.append(FORMULA_ARITY_X);
    sb.append(expr);
    sb.append(FORMULA_ARITY_Y);
    return sb.toString();
  }

  protected String[] parseFormula() {
    String formula, unbendingFormula, d;
    int idxOfSecondaryFormula = this.formula.indexOf(V9_SPLITTOR);
    if (idxOfSecondaryFormula >= 0) {
      String[] arr = StringUtils.split(this.formula, V9_SPLITTOR);
      formula = StringUtils.trimToNull(arr[0]);
      unbendingFormula = StringUtils.trimToNull(arr[1]);
      d = arr[2];
    } else {
      unbendingFormula = null;
      formula = this.formula;
      d = null;
    }
    return new String[]{formula, unbendingFormula, d};
  }
}
