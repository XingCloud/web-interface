package com.xingcloud.webinterface.sql.visitor;

import static com.xingcloud.webinterface.enums.Operator.EQ;
import static com.xingcloud.webinterface.plan.Plans.KEY_WORD_EVENT;
import static com.xingcloud.webinterface.plan.Plans.KEY_WORD_UID;
import static com.xingcloud.webinterface.plan.Plans.KEY_WORD_USER;
import static org.apache.drill.common.logical.data.Join.JoinType.ANTI;
import static org.apache.drill.common.logical.data.Join.JoinType.INNER;
import static org.apache.drill.common.util.DrillConstants.SE_HBASE;
import static org.apache.drill.common.util.DrillConstants.SE_MYSQL;
import static org.apache.drill.common.util.FieldReferenceBuilder.buildColumn;
import static org.apache.drill.common.util.FieldReferenceBuilder.buildTable;

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
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.data.Join;
import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.drill.common.logical.data.LogicalOperator;

import java.util.List;

/**
 * User: Z J Wu Date: 13-10-29 Time: 上午11:51 Package: com.xingcloud.webinterface.sql.visitor
 */
public class SegmentFromItemVisitor extends LogicalOperatorVisitor implements FromItemVisitor {

  private LogicalOperator logicalOperator;

  private FieldReference ref;

  private String storageEngine;

  private boolean isEventTable;

  private TableDescriptor tableDescriptor;

  private JoinDescriptor joinDescriptor;

  protected SegmentFromItemVisitor(FormulaQueryDescriptor descriptor, List<LogicalOperator> operators) {
    super(descriptor, operators);
  }

  public TableDescriptor getTableDescriptor() {
    return tableDescriptor;
  }

  public JoinDescriptor getJoinDescriptor() {
    return joinDescriptor;
  }

  public FieldReference getRef() {
    return ref;
  }

  public LogicalOperator getLogicalOperator() {
    return logicalOperator;
  }

  public String getStorageEngine() {
    return storageEngine;
  }

  public boolean isEventTable() {
    return isEventTable;
  }

  @Override
  public void visit(Table table) {
    String tableName = table.getName();
    isEventTable = SqlUtilsConstants.isEventTable(tableName);
    this.ref = isEventTable ? buildTable(KEY_WORD_EVENT) : buildTable(KEY_WORD_USER);
    this.storageEngine = isEventTable ? SE_HBASE : SE_MYSQL;
  }

  @Override
  public void visit(SubSelect subSelect) {
    SegmentSelectVisitor segmentSelectVisitor = new SegmentSelectVisitor(descriptor, operators);
    subSelect.getSelectBody().accept(segmentSelectVisitor);
    if (segmentSelectVisitor.isWrong()) {
      this.exception = segmentSelectVisitor.getException();
      return;
    }
    this.logicalOperator = segmentSelectVisitor.getLogicalOperator();
    this.tableDescriptor = segmentSelectVisitor.getTableDescriptor();
  }

  @Override
  public void visit(SubJoin join) {
    // Build left
    FromItem leftItem = join.getLeft();
    SegmentFromItemVisitor leftVisitor = new SegmentFromItemVisitor(descriptor, operators);
    leftItem.accept(leftVisitor);
    if (leftVisitor.isWrong()) {
      this.exception = leftVisitor.getException();
      return;
    }
    LogicalOperator leftLO = leftVisitor.getLogicalOperator();
    // Build right
    net.sf.jsqlparser.statement.select.Join rightJoin = join.getJoin();
    FromItem rightItem = rightJoin.getRightItem();
    SegmentFromItemVisitor rightVisitor = new SegmentFromItemVisitor(descriptor, operators);
    rightItem.accept(rightVisitor);
    if (rightVisitor.isWrong()) {
      this.exception = rightVisitor.getException();
      return;
    }
    LogicalOperator rightLO = rightVisitor.getLogicalOperator();

    // Build join conditions
    LogicalExpression left = buildColumn(leftItem.getAlias(), KEY_WORD_UID);
    LogicalExpression right = buildColumn(rightItem.getAlias(), KEY_WORD_UID);
    Join.JoinType joinType = getJoinType(rightJoin);
    if (joinType == null) {
      this.exception = new SegmentException("Unsupported join type - " + rightJoin);
      return;
    }
    JoinCondition[] joinConditions = new JoinCondition[]{new JoinCondition(EQ.getSqlOperator(), left, right)
    };
    logicalOperator = new Join(leftLO, rightLO, joinConditions, getJoinType(rightJoin));
    operators.add(logicalOperator);
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
    return this.ref != null;
  }

  public boolean isJoin() {
    return this.logicalOperator != null;
  }
}
