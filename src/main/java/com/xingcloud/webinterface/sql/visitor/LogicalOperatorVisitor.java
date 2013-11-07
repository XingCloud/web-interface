package com.xingcloud.webinterface.sql.visitor;

import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import org.apache.drill.common.logical.data.LogicalOperator;

import java.util.List;

/**
 * User: Z J Wu Date: 13-10-29 Time: 上午11:44 Package: com.xingcloud.webinterface.sql.visitor
 */
public abstract class LogicalOperatorVisitor extends FQDVisitor {

  protected List<LogicalOperator> operators;

  public LogicalOperatorVisitor(FormulaQueryDescriptor descriptor, List<LogicalOperator> operators) {
    super(descriptor);
    this.operators = operators;
  }

  public List<LogicalOperator> getOperators() {
    return operators;
  }
}
