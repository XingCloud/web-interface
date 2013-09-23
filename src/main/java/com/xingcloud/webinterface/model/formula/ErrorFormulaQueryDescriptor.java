package com.xingcloud.webinterface.model.formula;

import com.xingcloud.webinterface.exception.PlanException;
import org.apache.drill.common.logical.LogicalPlan;

public class ErrorFormulaQueryDescriptor extends FormulaQueryDescriptor {

  public ErrorFormulaQueryDescriptor() {
    super();
  }

  @Override public LogicalPlan toLogicalPlain() throws PlanException {
    return null;
  }

  public static final long serialVersionUID = -7580096818647296249L;

  private String errorMessage;
  private int errorCode;

  public ErrorFormulaQueryDescriptor(int errorCode) {
    super();
    this.errorCode = errorCode;
  }

  public ErrorFormulaQueryDescriptor(String errorMessage) {
    super();
    this.errorMessage = errorMessage;
  }

  public ErrorFormulaQueryDescriptor(String errorMessage, int errorCode) {
    super();
    this.errorMessage = errorMessage;
    this.errorCode = errorCode;
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    // TODO Auto-generated method stub

  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public void setErrorMessage(String errorMessage) {
    this.errorMessage = errorMessage;
  }

  public int getErrorCode() {
    return errorCode;
  }

  public void setErrorCode(int errorCode) {
    this.errorCode = errorCode;
  }

  @Override
  public String toKey() {
    return "ErrorFormulaQueryDescriptor";
  }

  @Override
  public String toKey(boolean ignore) {
    return toKey();
  }

}
