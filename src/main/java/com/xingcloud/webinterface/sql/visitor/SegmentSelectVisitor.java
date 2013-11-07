package com.xingcloud.webinterface.sql.visitor;

import static com.xingcloud.webinterface.enums.GroupByType.USER_PROPERTIES;
import static com.xingcloud.webinterface.enums.Operator.EQ;
import static com.xingcloud.webinterface.enums.Operator.GT;
import static com.xingcloud.webinterface.enums.Operator.GTE;
import static com.xingcloud.webinterface.enums.Operator.LT;
import static com.xingcloud.webinterface.enums.Operator.LTE;
import static com.xingcloud.webinterface.enums.Operator.SGMT;
import static com.xingcloud.webinterface.enums.SegmentTableType.E;
import static com.xingcloud.webinterface.enums.SegmentTableType.U;
import static com.xingcloud.webinterface.plan.Plans.DEFAULT_DRILL_CONFIG;
import static com.xingcloud.webinterface.plan.Plans.KEY_WORD_EVENT;
import static com.xingcloud.webinterface.plan.Plans.KEY_WORD_TIMESTAMP;
import static com.xingcloud.webinterface.plan.Plans.KEY_WORD_UID;
import static com.xingcloud.webinterface.plan.Plans.KEY_WORD_VALUE;
import static com.xingcloud.webinterface.plan.Plans.getChainedMysqlSegmentScan2;
import static com.xingcloud.webinterface.plan.Plans.toBinaryExpression;
import static com.xingcloud.webinterface.plan.Plans.toEventTableName;
import static com.xingcloud.webinterface.sql.SqlUtilsConstants.DATE_FIELD;
import static com.xingcloud.webinterface.sql.SqlUtilsConstants.EVENT_FIELD;
import static com.xingcloud.webinterface.sql.SqlUtilsConstants.USING_SGMT_FUNC;
import static org.apache.drill.common.expression.ExpressionPosition.UNKNOWN;
import static org.apache.drill.common.util.FieldReferenceBuilder.buildColumn;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xingcloud.events.XEvent;
import com.xingcloud.events.XEventException;
import com.xingcloud.webinterface.enums.Interval;
import com.xingcloud.webinterface.enums.Operator;
import com.xingcloud.webinterface.enums.SegmentTableType;
import com.xingcloud.webinterface.exception.PlanException;
import com.xingcloud.webinterface.exception.SegmentException;
import com.xingcloud.webinterface.model.BeginEndDatePair;
import com.xingcloud.webinterface.model.formula.CommonFormulaQueryDescriptor;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import com.xingcloud.webinterface.model.formula.GroupByFormulaQueryDescriptor;
import com.xingcloud.webinterface.plan.ScanFilter;
import com.xingcloud.webinterface.plan.ScanSelection;
import com.xingcloud.webinterface.sql.SegmentDescriptor;
import com.xingcloud.webinterface.utils.DateSplitter;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.Union;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.logical.data.Scan;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * User: Z J Wu Date: 13-10-28 Time: 下午5:22 Package: com.xingcloud.webinterface.sql.visitor
 */
public class SegmentSelectVisitor extends LogicalOperatorVisitor implements SelectVisitor {

  private LogicalOperator logicalOperator;

  private Map<String, Map<Operator, Object>> segmentToStringMap;

  private SegmentDescriptor segmentDescriptor;

  public SegmentSelectVisitor(FormulaQueryDescriptor descriptor, List<LogicalOperator> operators) {
    super(descriptor, operators);
  }

  public LogicalOperator getLogicalOperator() {
    return logicalOperator;
  }

  public Map<String, Map<Operator, Object>> getSegmentToStringMap() {
    return segmentToStringMap;
  }

  public SegmentDescriptor getSegmentDescriptor() {
    return segmentDescriptor;
  }

  @Override public void visit(PlainSelect plainSelect) {
    FromItem fi = plainSelect.getFromItem();
    SegmentFromItemVisitor segmentFromItemVisitor = new SegmentFromItemVisitor(descriptor, operators);
    fi.accept(segmentFromItemVisitor);
    if (segmentFromItemVisitor.isWrong()) {
      this.exception = segmentFromItemVisitor.getException();
      return;
    }
    if (segmentFromItemVisitor.isSingleTable()) {
      String storageEngine = segmentFromItemVisitor.getStorageEngine();
      boolean isEventTable = segmentFromItemVisitor.isEventTable();
      Expression whereClause = plainSelect.getWhere();

      Map<String, Map<Operator, Object>> whereClausesMap = new TreeMap<String, Map<Operator, Object>>();
      this.segmentToStringMap = whereClausesMap;
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
      this.segmentDescriptor = new SegmentDescriptor(segmentTableType, whereClausesMap);
      if (E.equals(segmentTableType)) {
        toEventLogicalOperator(storageEngine, whereClausesMap, segmentFromItemVisitor.getRef());
      } else {
        killSGMT(whereClausesMap);
        toUserLogicalOperator(whereClausesMap);
      }
    } else {
      if (segmentFromItemVisitor.isOK()) {
        this.logicalOperator = segmentFromItemVisitor.getLogicalOperator();
      } else {
        this.exception = segmentFromItemVisitor.getException();
      }
      this.segmentDescriptor = segmentFromItemVisitor.getSegmentDescriptor();
    }
  }

  private void killSGMT(Map<String, Map<Operator, Object>> whereClausesMap) {
    Map<Operator, Object> valMap;
    for (Map.Entry<String, Map<Operator, Object>> entry : whereClausesMap.entrySet()) {
      valMap = entry.getValue();
      if (valMap.containsValue(USING_SGMT_FUNC)) {
        valMap.clear();
        valMap.put(SGMT, descriptor.getInputBeginDate());
      }
    }
  }

  private void toEventLogicalOperator(String storageEngine, Map<String, Map<Operator, Object>> whereClausesMap,
                                      FieldReference ref) {
    ScanSelection ss;
    ScanSelection[] sss;
    ScanFilter sf;
    JSONOptions selections = null;

    String[] additionalProjections;
    if (descriptor.isCommon()) {
      CommonFormulaQueryDescriptor cfqd = (CommonFormulaQueryDescriptor) descriptor;
      boolean min5HourQuery = cfqd.getInterval().getDays() < 1;
      additionalProjections = min5HourQuery ? new String[]{KEY_WORD_TIMESTAMP} : null;
    } else {
      GroupByFormulaQueryDescriptor gbfqd = (GroupByFormulaQueryDescriptor) descriptor;
      boolean userPropertyGroupBy = USER_PROPERTIES.equals(gbfqd.getGroupByType());
      additionalProjections = userPropertyGroupBy ? null : new String[]{KEY_WORD_EVENT + gbfqd.getGroupBy()};
    }
    NamedExpression[] projections;
    boolean emptyProjections = ArrayUtils.isEmpty(additionalProjections);
    if (emptyProjections) {
      projections = new NamedExpression[2];
    } else {
      projections = new NamedExpression[2 + additionalProjections.length];
    }
    FieldReference fr = buildColumn(KEY_WORD_UID);
    projections[0] = new NamedExpression(fr, fr);
    fr = buildColumn(KEY_WORD_VALUE);
    projections[1] = new NamedExpression(fr, fr);
    if (!emptyProjections) {
      for (int i = 0; i < additionalProjections.length; i++) {
        fr = buildColumn(additionalProjections[i]);
        projections[2 + i] = new NamedExpression(fr, fr);
      }
    }

    Map<Operator, Object> dateConditionMap = whereClausesMap.get(DATE_FIELD);
    if (MapUtils.isEmpty(dateConditionMap)) {
      this.exception = new SegmentException("Date range must be assigned in event sql.");
      return;
    }
    Object eventObj = whereClausesMap.get(EVENT_FIELD).get(EQ);
    String eventString = eventObj == null ? "*" : eventObj.toString();
    XEvent xEvent;
    try {
      xEvent = XEvent.buildXEvent(descriptor.getProjectId(), eventString);
    } catch (XEventException e) {
      this.exception = e;
      return;
    }
    if (xEvent == null) {
      this.exception = new XEventException("No such event - " + xEvent);
      return;
    }

    Map<String, LogicalExpression> logicalExpressionMap;
    if (xEvent.isAll()) {
      logicalExpressionMap = new HashMap<String, LogicalExpression>(1);
    } else {
      logicalExpressionMap = new HashMap<String, LogicalExpression>(xEvent.maxLevel() + 1);
    }
    String[] arr = xEvent.getEventArray();
    for (int i = 0; i < arr.length; i++) {
      if (StringUtils.isNotBlank(arr[i])) {
        logicalExpressionMap.put(KEY_WORD_EVENT + i, new ValueExpressions.QuotedString(arr[i], UNKNOWN));
      }
    }

    Object val1 = dateConditionMap.get(EQ), val2;
    String beginDate, endDate;
    if (val1 == null) {
      val1 = dateConditionMap.get(GTE);
      if (val1 == null) {
        val1 = dateConditionMap.get(GT);
        if (val1 == null) {
          this.exception = new SegmentException("Cannot parse lower bound in event table.");
          return;
        }
      }
      val2 = dateConditionMap.get(LTE);
      if (val2 == null) {
        val2 = dateConditionMap.get(LT);
        if (val2 == null) {
          this.exception = new SegmentException("Cannot parse upper bound in event table.");
          return;
        }
      }
      endDate = val2.toString();
    } else {
      endDate = val1.toString();
    }
    beginDate = val1.toString();

    // 拆解日期至单天, 保证每个selection都是连续的范围
    List<BeginEndDatePair> dates;
    try {
      dates = DateSplitter.split2Pairs(beginDate, endDate, Interval.DAY);
    } catch (ParseException e) {
      this.exception = e;
      return;
    }
    if (CollectionUtils.isEmpty(dates)) {
      this.exception = new SegmentException("Cannot parse invalid date period.");
      return;
    }
    sss = new ScanSelection[dates.size()];

    int cnt = 0;
    String d;
    for (BeginEndDatePair datePair : dates) {
      d = datePair.getBeginDate();
      logicalExpressionMap.put(DATE_FIELD, new ValueExpressions.QuotedString(d.replace("-", ""), UNKNOWN));

      try {
        sf = new ScanFilter(toBinaryExpression(logicalExpressionMap, "and"));
      } catch (PlanException e) {
        this.exception = e;
        return;
      }

      ss = new ScanSelection(toEventTableName(descriptor.getProjectId()), sf, projections);
      sss[cnt] = ss;
      ++cnt;
    }
    ObjectMapper mapper = DEFAULT_DRILL_CONFIG.getMapper();
    try {
      selections = mapper.readValue(mapper.writeValueAsBytes(sss), JSONOptions.class);
    } catch (IOException e) {
      this.exception = e;
    }
    this.logicalOperator = new Scan(storageEngine, selections, ref);
    operators.add(logicalOperator);
  }

  private void toUserLogicalOperator(Map<String, Map<Operator, Object>> whereClausesMap) {
    try {
      this.logicalOperator = getChainedMysqlSegmentScan2(this.descriptor.getProjectId(), this.operators,
                                                         whereClausesMap);
    } catch (PlanException e) {
      this.exception = e;
    }
  }

  // Current not supported.
  @Override public void visit(Union union) {
    this.exception = new SegmentException("Unsupported operation - " + union);
  }
}
