package com.xingcloud.webinterface.sql.visitor;

import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;

/**
 * User: Z J Wu Date: 13-10-29 Time: 上午11:44 Package: com.xingcloud.webinterface.sql.visitor
 */
public abstract class LogicalOperatorVisitor extends FQDVisitor {

  public LogicalOperatorVisitor(FormulaQueryDescriptor descriptor) {
    super(descriptor);
  }

}
