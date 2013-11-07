package com.xingcloud.webinterface.sql.visitor;

import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;

/**
 * User: Z J Wu Date: 13-10-30 Time: 上午9:55 Package: com.xingcloud.webinterface.sql.visitor
 */
public abstract class FQDVisitor {
  protected FormulaQueryDescriptor descriptor;
  protected Exception exception;

  public FQDVisitor(FormulaQueryDescriptor descriptor) {
    this.descriptor = descriptor;
  }

  public FormulaQueryDescriptor getDescriptor() {
    return descriptor;
  }

  public boolean isOK() {
    return this.exception == null;
  }

  public boolean isWrong() {
    return !isOK();
  }

  public Exception getException() {
    return exception;
  }

}
