package com.xingcloud.webinterface.sql.visitor;

import com.xingcloud.webinterface.exception.SegmentException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.Union;
import org.apache.drill.common.expression.LogicalExpression;

/**
 * User: Z J Wu Date: 13-10-28 Time: 下午5:22 Package: com.xingcloud.webinterface.sql.visitor
 */
public class SegmentSelectVisitor implements net.sf.jsqlparser.statement.select.SelectVisitor {

  private LogicalExpression segmentLogicalExpression;

  private Exception exception;

  public boolean isOK() {
    return exception == null;
  }

  public LogicalExpression getSegmentLogicalExpression() {
    return segmentLogicalExpression;
  }

  private boolean isSingleTable(FromItem fromItem) {
    return fromItem instanceof Table;
  }

  private boolean isJoin(FromItem fromItem) {
    return fromItem instanceof SubJoin;
  }

  @Override public void visit(PlainSelect plainSelect) {
    FromItem fi = plainSelect.getFromItem();
    if (isSingleTable(fi)) {
      visitTable(plainSelect);
    } else if (isJoin(fi)) {
      visitJoin(plainSelect);
    } else {
      this.exception = new SegmentException("Unsupported operation - " + fi);
    }
  }

  private void visitTable(PlainSelect plainSelect) {
    Expression expression = plainSelect.getWhere();
    expression.accept(new ExprVisitor());
  }

  private void visitJoin(PlainSelect plainSelect) {
    SubJoin sj= (SubJoin) plainSelect.getFromItem();
    System.out.println(sj.getLeft().getAlias());
  }

  @Override public void visit(Union union) {

  }
}
