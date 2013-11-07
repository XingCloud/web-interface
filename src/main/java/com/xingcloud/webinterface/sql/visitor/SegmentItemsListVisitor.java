package com.xingcloud.webinterface.sql.visitor;

import com.xingcloud.mysql.UserProp;
import com.xingcloud.webinterface.exception.SegmentException;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.statement.select.SubSelect;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * User: Z J Wu Date: 13-11-4 Time: 下午5:42 Package: com.xingcloud.webinterface.sql.visitor
 */
public class SegmentItemsListVisitor extends FQDVisitor implements ItemsListVisitor {
  private Set<Object> values;
  private UserProp userProp;

  public SegmentItemsListVisitor(FormulaQueryDescriptor descriptor, UserProp userProp) {
    super(descriptor);
    this.userProp = userProp;
  }

  public Set<Object> getValues() {
    return values;
  }

  @Override
  public void visit(SubSelect subSelect) {
    this.exception = new SegmentException("Unsupported operation SubSelect in item list visitor.");
  }

  @Override
  public void visit(ExpressionList expressionList) {
    List expressions = expressionList.getExpressions();
    values = new TreeSet<Object>();
    Expression expression;
    InOperatorExpressionVisitor visitor;
    for (Object expressionObject : expressions) {
      expression = (Expression) expressionObject;
      visitor = new InOperatorExpressionVisitor(descriptor, userProp);
      expression.accept(visitor);
      if (visitor.isWrong()) {
        this.exception = visitor.getException();
        return;
      }
      values.add(visitor.getInItemValue());
    }
  }
}
