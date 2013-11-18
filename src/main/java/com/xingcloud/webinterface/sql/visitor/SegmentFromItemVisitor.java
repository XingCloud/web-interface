package com.xingcloud.webinterface.sql.visitor;

import static org.apache.drill.common.logical.data.Join.JoinType.ANTI;
import static org.apache.drill.common.logical.data.Join.JoinType.INNER;

import com.xingcloud.webinterface.exception.SegmentException;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import com.xingcloud.webinterface.sql.SqlUtilsConstants;
import com.xingcloud.webinterface.sql.desc.JoinDescriptor;
import com.xingcloud.webinterface.sql.desc.TableDescriptor;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import org.apache.drill.common.logical.data.Join;

/**
 * User: Z J Wu Date: 13-10-29 Time: 上午11:51 Package: com.xingcloud.webinterface.sql.visitor
 */
public class SegmentFromItemVisitor extends LogicalOperatorVisitor implements FromItemVisitor {

  private boolean singleTalbe = false;

  private boolean isEventTable;

  private TableDescriptor tableDescriptor;

  private JoinDescriptor joinDescriptor;

  protected SegmentFromItemVisitor(FormulaQueryDescriptor descriptor) {
    super(descriptor);
  }

  public TableDescriptor getTableDescriptor() {
    return tableDescriptor;
  }

  public JoinDescriptor getJoinDescriptor() {
    return joinDescriptor;
  }

  public boolean isEventTable() {
    return isEventTable;
  }

  @Override
  public void visit(Table table) {
    String tableName = table.getName();
    isEventTable = SqlUtilsConstants.isEventTable(tableName);
    this.singleTalbe = true;
  }

  @Override
  public void visit(SubSelect subSelect) {
    SegmentSelectVisitor segmentSelectVisitor = new SegmentSelectVisitor(descriptor);
    subSelect.getSelectBody().accept(segmentSelectVisitor);
    if (segmentSelectVisitor.isWrong()) {
      this.exception = segmentSelectVisitor.getException();
      return;
    }
    this.tableDescriptor = segmentSelectVisitor.getTableDescriptor();
  }

  @Override
  public void visit(SubJoin join) {
    // Build left
    FromItem leftItem = join.getLeft();
    SegmentFromItemVisitor leftVisitor = new SegmentFromItemVisitor(descriptor);
    leftItem.accept(leftVisitor);
    if (leftVisitor.isWrong()) {
      this.exception = leftVisitor.getException();
      return;
    }
    // Build right
    net.sf.jsqlparser.statement.select.Join rightJoin = join.getJoin();
    FromItem rightItem = rightJoin.getRightItem();
    SegmentFromItemVisitor rightVisitor = new SegmentFromItemVisitor(descriptor);
    rightItem.accept(rightVisitor);
    if (rightVisitor.isWrong()) {
      this.exception = rightVisitor.getException();
      return;
    }
    // Build join conditions
    Join.JoinType joinType = getJoinType(rightJoin);
    if (joinType == null) {
      this.exception = new SegmentException("Unsupported join type - " + rightJoin);
      return;
    }
    this.joinDescriptor = new JoinDescriptor(joinType, leftVisitor.getTableDescriptor(),
                                             rightVisitor.getTableDescriptor());
  }

  public Join.JoinType getJoinType(net.sf.jsqlparser.statement.select.Join join) {
    if (join.isAnti()) {
      return ANTI;
    } else if (join.isLeft()) {
      return INNER;
    } else if (join.isInner()) {
      return null;
    } else if (join.isRight()) {
      return null;
    } else if (join.isOuter()) {
      return null;
    } else {
      return null;
    }
  }

  public boolean isSingleTable() {
    return singleTalbe;
  }
}
