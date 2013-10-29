package com.xingcloud.webinterface.sql.visitor;

import static org.apache.drill.common.util.FieldReferenceBuilder.buildColumn;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.schema.Column;
import org.apache.drill.common.expression.LogicalExpression;

/**
 * User: Z J Wu Date: 13-10-28 Time: 下午5:26 Package: com.xingcloud.webinterface.sql.visitor
 */
public class ExprVisitor extends AbstractExprVisitor {
  private LogicalExpression logicalExpression;

  public LogicalExpression getLogicalExpression() {
    return logicalExpression;
  }

  public void visitBinaryExpression(BinaryExpression binaryExpression) {
    binaryExpression.getLeftExpression().accept(this);
    binaryExpression.getRightExpression().accept(this);
  }

  @Override public void visit(Function function) {
    String funcName = function.getName();
  }

  @Override public void visit(Column column) {
    this.logicalExpression = buildColumn(column.getColumnName());
  }

  @Override public void visit(MinorThanEquals minorThanEquals) {
    super.visit(minorThanEquals);
  }

  @Override public void visit(MinorThan minorThan) {
    super.visit(minorThan);
  }

  @Override public void visit(InExpression inExpression) {
    super.visit(inExpression);
  }

  @Override public void visit(GreaterThanEquals greaterThanEquals) {
    super.visit(greaterThanEquals);
  }

  @Override public void visit(GreaterThan greaterThan) {
    super.visit(greaterThan);
  }

  @Override public void visit(EqualsTo equalsTo) {
    super.visit(equalsTo);
  }

  @Override public void visit(OrExpression orExpression) {
    super.visit(orExpression);
  }

  @Override public void visit(AndExpression andExpression) {
    super.visit(andExpression);
  }

}
