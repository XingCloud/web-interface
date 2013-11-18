package com.xingcloud.webinterface.sql.visitor;

import static com.xingcloud.webinterface.enums.Interval.HOUR;
import static com.xingcloud.webinterface.enums.Interval.MIN5;
import static com.xingcloud.webinterface.enums.Operator.SGMT300;
import static com.xingcloud.webinterface.enums.Operator.SGMT3600;
import static com.xingcloud.webinterface.enums.SegmentTableType.E;
import static com.xingcloud.webinterface.enums.SegmentTableType.U;
import static com.xingcloud.webinterface.sql.SqlUtilsConstants.USING_SGMT_FUNC;

import com.xingcloud.webinterface.enums.Interval;
import com.xingcloud.webinterface.enums.Operator;
import com.xingcloud.webinterface.enums.SegmentTableType;
import com.xingcloud.webinterface.exception.SegmentException;
import com.xingcloud.webinterface.model.formula.CommonFormulaQueryDescriptor;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import com.xingcloud.webinterface.sql.desc.JoinDescriptor;
import com.xingcloud.webinterface.sql.desc.TableDescriptor;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.Union;
import org.apache.commons.collections.MapUtils;

import java.util.Map;
import java.util.TreeMap;

/**
 * User: Z J Wu Date: 13-10-28 Time: 下午5:22 Package: com.xingcloud.webinterface.sql.visitor
 */
public class SegmentSelectVisitor extends LogicalOperatorVisitor implements SelectVisitor {

  //  private LogicalOperator logicalOperator;
  private JoinDescriptor joinDescriptor;
  private TableDescriptor tableDescriptor;

  public JoinDescriptor getJoinDescriptor() {
    return joinDescriptor;
  }

  public TableDescriptor getTableDescriptor() {
    return tableDescriptor;
  }

  public SegmentSelectVisitor(FormulaQueryDescriptor descriptor) {
    super(descriptor);
  }

  @Override
  public void visit(PlainSelect plainSelect) {
    FromItem fi = plainSelect.getFromItem();
    SegmentFromItemVisitor segmentFromItemVisitor = new SegmentFromItemVisitor(descriptor);
    fi.accept(segmentFromItemVisitor);
    if (segmentFromItemVisitor.isWrong()) {
      this.exception = segmentFromItemVisitor.getException();
      return;
    }
    if (segmentFromItemVisitor.isSingleTable()) {
      boolean isEventTable = segmentFromItemVisitor.isEventTable();
      Expression whereClause = plainSelect.getWhere();
      if (whereClause == null) {
        this.exception = new SegmentException("One query must has at least one where clause.");
        return;
      }

      Map<String, Map<Operator, Object>> whereClausesMap = new TreeMap<String, Map<Operator, Object>>();
      SegmentTableType segmentTableType = isEventTable ? E : U;
      SegmentExpressionVisitor exprVisitor = new SegmentExpressionVisitor(descriptor, segmentTableType,
                                                                          whereClausesMap);
      whereClause.accept(exprVisitor);
      if (exprVisitor.isWrong()) {
        this.exception = exprVisitor.getException();
        return;
      }
      if (MapUtils.isEmpty(whereClausesMap)) {
        this.exception = new SegmentException("There is no any property in where clauses.");
        return;
      }
      if (E.equals(segmentTableType)) {
//        toEventLogicalOperator(storageEngine, whereClausesMap, segmentFromItemVisitor.getRef());
      } else {
        if (descriptor.isCommon()) {
          replaceSGMTPlaceholder(whereClausesMap, ((CommonFormulaQueryDescriptor) descriptor).getInterval());
        }
//        toUserLogicalOperator(whereClausesMap);
      }
      this.tableDescriptor = TableDescriptor.create(whereClausesMap, segmentTableType);
    } else {
      if (segmentFromItemVisitor.isOK()) {
//        this.logicalOperator = segmentFromItemVisitor.getLogicalOperator();
      } else {
        this.exception = segmentFromItemVisitor.getException();
        return;
      }
      this.joinDescriptor = segmentFromItemVisitor.getJoinDescriptor();
    }
  }

  private void replaceSGMTPlaceholder(Map<String, Map<Operator, Object>> whereClausesMap, Interval interval) {
    Map<Operator, Object> valMap;
    for (Map.Entry<String, Map<Operator, Object>> entry : whereClausesMap.entrySet()) {
      valMap = entry.getValue();
      if (valMap.containsValue(USING_SGMT_FUNC)) {
        valMap.clear();
        if (HOUR.equals(interval)) {
          valMap.put(SGMT3600, descriptor.getInputBeginDate());
        } else if (MIN5.equals(interval)) {
          valMap.put(SGMT300, descriptor.getInputBeginDate());
        }
      }
    }
  }

  // Current not supported.
  @Override public void visit(Union union) {
    this.exception = new SegmentException("Unsupported operation - " + union);
  }
}
