package com.xingcloud.webinterface.sql.visitor;

import static com.xingcloud.webinterface.calculate.func.DateAddFunction.DATE_ADD_FUNCTION_NAME;

import com.xingcloud.mysql.UserProp;
import com.xingcloud.webinterface.exception.ExpressionEvaluationException;
import com.xingcloud.webinterface.exception.SegmentException;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;

/**
 * User: Z J Wu Date: 13-11-4 Time: 下午6:08 Package: com.xingcloud.webinterface.sql.visitor
 */
public class InOperatorExpressionVisitor extends AbstractExprVisitor {

  private Object inItemValue;

  private UserProp userProp;

  public InOperatorExpressionVisitor(FormulaQueryDescriptor descriptor, UserProp userProp) {
    super(descriptor);
    this.userProp = userProp;
  }

  public Object getInItemValue() {
    return inItemValue;
  }

  @Override public void visit(Function function) {
    String funcName = function.getName();
    if (DATE_ADD_FUNCTION_NAME.equals(funcName)) {
      try {
        this.inItemValue = visitDateAddFunction(function);
      } catch (ExpressionEvaluationException e) {
        this.exception = e;
      }
    } else {
      this.exception = new SegmentException("Unsupported function - " + funcName);
    }
  }

  @Override public void visit(DoubleValue doubleValue) {
    this.inItemValue = doubleValue.getValue();
  }

  @Override public void visit(LongValue longValue) {
    this.inItemValue = longValue.getValue();
  }

  @Override public void visit(StringValue stringValue) {
    visit(stringValue.getValue());
  }

  @Override public void visit(Column column) {
    visit(column.getColumnName());
  }

  private void visit(String string) {
    switch (userProp.getPropType()) {
      case sql_bigint:
        this.inItemValue = Long.valueOf(string);
        break;
      default:
        this.inItemValue = string;
        break;
    }
  }
}
