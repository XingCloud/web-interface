package com.xingcloud.webinterface.model.formula;

import static com.xingcloud.basic.Constants.SEPARATOR_CHAR_CACHE;
import static com.xingcloud.webinterface.enums.Interval.HOUR;
import static com.xingcloud.webinterface.enums.Interval.MIN5;
import static com.xingcloud.webinterface.plan.Plans2.DFR;
import static com.xingcloud.webinterface.plan.Plans2.KEY_WORD_DIMENSION;
import static com.xingcloud.webinterface.plan.Plans2.KEY_WORD_SGMT;
import static com.xingcloud.webinterface.plan.Plans2.KEY_WORD_TIMESTAMP;
import static com.xingcloud.webinterface.plan.Plans2.KEY_WORD_UID;
import static com.xingcloud.webinterface.plan.Plans2.KEY_WORD_VALUE;
import static com.xingcloud.webinterface.plan.Plans2.buildPlanProperties;
import static com.xingcloud.webinterface.plan.Plans2.getChainedMysqlSegmentScan;
import static com.xingcloud.webinterface.plan.Plans2.getChainedSegmentFilter;
import static com.xingcloud.webinterface.plan.Plans2.getEventScan;
import static com.xingcloud.webinterface.plan.Plans2.getStore;
import static org.apache.drill.common.enums.Aggregator.COUNT;
import static org.apache.drill.common.enums.Aggregator.COUNT_DISTINCT;
import static org.apache.drill.common.enums.Aggregator.SUM;
import static org.apache.drill.common.util.FieldReferenceBuilder.buildColumn;

import com.xingcloud.webinterface.enums.CommonQueryType;
import com.xingcloud.webinterface.enums.Interval;
import com.xingcloud.webinterface.enums.Operator;
import com.xingcloud.webinterface.exception.PlanException;
import com.xingcloud.webinterface.model.Filter;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.common.logical.data.CollapsingAggregate;
import org.apache.drill.common.logical.data.Join;
import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.logical.data.Project;
import org.apache.drill.common.logical.data.Segment;
import org.apache.drill.common.logical.data.Store;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CommonFormulaQueryDescriptor extends FormulaQueryDescriptor {
  protected Interval interval;

  protected CommonQueryType commonQueryType;

  protected boolean functionalSegment;

  private boolean hasFunctionalSegment() {
    return functionalSegment;
  }

  public CommonFormulaQueryDescriptor() {
    super();
  }

  public CommonFormulaQueryDescriptor(String projectId, String realBeginDate, String realEndDate, String event,
                                      String segment, Filter filter, double samplingRate, Interval interval,
                                      CommonQueryType commonQueryType) {
    super(projectId, realBeginDate, realEndDate, event, segment, filter, samplingRate);
    this.interval = interval;
    this.commonQueryType = commonQueryType;
  }

  public CommonFormulaQueryDescriptor(String projectId, String realBeginDate, String realEndDate, String event,
                                      String segment, Filter filter, double samplingRate, String inputBeginDate,
                                      String inputEndDate, Interval interval, CommonQueryType commonQueryType) {
    super(projectId, realBeginDate, realEndDate, event, segment, filter, samplingRate, inputBeginDate, inputEndDate);
    this.interval = interval;
    this.commonQueryType = commonQueryType;
  }

  protected String toKey() {
    return toKey(true);
  }

  @Override
  protected String toKey(boolean ignore) {
    StringBuilder sb = toKeyGeneric();
    if (sb == null) {
      return null;
    }
    sb.append(SEPARATOR_CHAR_CACHE);
    sb.append(getInterval());

    if (!ignore) {
      toStringGeneric(sb);
      sb.append(SEPARATOR_CHAR_CACHE);
      sb.append(getCommonQueryType());
    }
    return sb.toString();
  }

  public void setFunctionalSegment(boolean functionalSegment) {
    this.functionalSegment = functionalSegment;
  }

  public Interval getInterval() {
    return interval;
  }

  public void setInterval(Interval interval) {
    this.interval = interval;
  }

  public CommonQueryType getCommonQueryType() {
    return commonQueryType;
  }

  public void setCommonQueryType(CommonQueryType commonQueryType) {
    this.commonQueryType = commonQueryType;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((commonQueryType == null) ? 0 : commonQueryType.hashCode()
    );
    result = prime * result + ((interval == null) ? 0 : interval.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    CommonFormulaQueryDescriptor other = (CommonFormulaQueryDescriptor) obj;
    if (commonQueryType != other.commonQueryType)
      return false;
    if (interval != other.interval)
      return false;
    return true;
  }

  @Override
  public String toString() {
    return toKey(false);
  }

  @Override
  public LogicalPlan toLogicalPlain() throws PlanException {
    boolean hasFunctionalSegment = hasFunctionalSegment();
    List<LogicalOperator> logicalOperators = new ArrayList<LogicalOperator>();
    boolean min5HourQuery = this.interval.getDays() < 1;
    String[] additionalProjections = min5HourQuery ? new String[]{KEY_WORD_TIMESTAMP} : null;
    LogicalOperator eventTableScan = getEventScan(this.projectId, this.event, this.realBeginDate, this.realEndDate,
                                                  additionalProjections);
    logicalOperators.add(eventTableScan);
    Join join;
    JoinCondition[] joinConditions;

    LogicalOperator scanRoot, segmentLogicalOperator;
    if (hasSegment()) {
      Map<String, Map<Operator, Object>> segmentMap = getSegmentMap();
      if (segmentMap.containsKey("N/A")) {
        return null;
      }
      segmentLogicalOperator = getChainedMysqlSegmentScan(this.projectId, logicalOperators, segmentMap);
      joinConditions = new JoinCondition[1];
      joinConditions[0] = new JoinCondition("==", buildColumn(KEY_WORD_UID), buildColumn(KEY_WORD_UID));
      join = new Join(segmentLogicalOperator, eventTableScan, joinConditions, Join.JoinType.INNER);
      logicalOperators.add(join);
      scanRoot = join;
    } else {
      scanRoot = eventTableScan;
    }
    String segmentFunc;
    switch (this.interval) {
      case MIN5:
        segmentFunc = "div300";
        break;
      case HOUR:
        segmentFunc = "div3600";
        break;
      default:
        segmentFunc = null;
    }

    org.apache.drill.common.logical.data.Filter logicalFilter = null;
    if (hasFunctionalSegment) {
      LogicalExpression filterExpression = getChainedSegmentFilter(getSegmentMap(), segmentFunc, KEY_WORD_TIMESTAMP);
      logicalFilter = new org.apache.drill.common.logical.data.Filter(filterExpression);
      logicalFilter.setInput(scanRoot);
      logicalOperators.add(logicalFilter);
    }

    // build min5 groupby
    Segment segment = null;
    LogicalExpression singleGroupByLE;
    if (min5HourQuery) {
      singleGroupByLE = DFR.createExpression(segmentFunc, ExpressionPosition.UNKNOWN, buildColumn(KEY_WORD_TIMESTAMP));
      FieldReference fr = buildColumn(KEY_WORD_SGMT), fr2 = buildColumn(KEY_WORD_DIMENSION);
      segment = new Segment(new NamedExpression[]{new NamedExpression(singleGroupByLE, fr2)}, fr);
      if (logicalFilter == null) {
        segment.setInput(scanRoot);
      } else {
        segment.setInput(logicalFilter);
      }
      logicalOperators.add(segment);
    }

    CollapsingAggregate collapsingAggregate;
    FieldReference within = (segment == null ? null : segment.getName()), target = null;
    FieldReference[] carryovers = null;
    NamedExpression[] namedExpressions = new NamedExpression[3];

    if (min5HourQuery) {
      carryovers = new FieldReference[1];
      carryovers[0] = buildColumn(KEY_WORD_DIMENSION);
    }

    FunctionCall fc;
    FieldReference aggrOn = buildColumn(KEY_WORD_UID);
    fc = (FunctionCall) DFR.createExpression(COUNT.getKeyWord(), ExpressionPosition.UNKNOWN, aggrOn);
    namedExpressions[0] = new NamedExpression(fc, buildColumn("count"));

    fc = (FunctionCall) DFR.createExpression(COUNT_DISTINCT.getKeyWord(), ExpressionPosition.UNKNOWN, aggrOn);
    namedExpressions[1] = new NamedExpression(fc, buildColumn("user_num"));

    aggrOn = buildColumn(KEY_WORD_VALUE);
    fc = (FunctionCall) DFR.createExpression(SUM.getKeyWord(), ExpressionPosition.UNKNOWN, aggrOn);
    namedExpressions[2] = new NamedExpression(fc, buildColumn("sum"));
    collapsingAggregate = new CollapsingAggregate(within, target, carryovers, namedExpressions);

    if (min5HourQuery) {
      collapsingAggregate.setInput(segment);
    } else {
      collapsingAggregate.setInput(scanRoot);
    }
    logicalOperators.add(collapsingAggregate);

    String queryId = getKey();
    NamedExpression[] selections = min5HourQuery ? new NamedExpression[5] : new NamedExpression[4];
    LogicalExpression constQueryId = new ValueExpressions.QuotedString(queryId, ExpressionPosition.UNKNOWN);
    selections[0] = new NamedExpression(constQueryId, buildColumn("query_id"));
    if (min5HourQuery) {
      switch (this.interval) {
        case MIN5:
          segmentFunc = MIN5.name().toLowerCase();
          break;
        case HOUR:
          segmentFunc = HOUR.name().toLowerCase();
          break;
        default:
          throw new PlanException(
            "Cannot discriminate function because interval is not ateappropriate - " + this.interval);
      }
      LogicalExpression finalFunc = DFR
        .createExpression(segmentFunc, ExpressionPosition.UNKNOWN, buildColumn("dimension"));
      selections[1] = new NamedExpression(finalFunc, buildColumn("dimension"));
      selections[2] = new NamedExpression(buildColumn("count"), buildColumn("count"));
      selections[3] = new NamedExpression(buildColumn("user_num"), buildColumn("user_num"));
      selections[4] = new NamedExpression(buildColumn("sum"), buildColumn("sum"));
    } else {
      selections[1] = new NamedExpression(buildColumn("count"), buildColumn("count"));
      selections[2] = new NamedExpression(buildColumn("user_num"), buildColumn("user_num"));
      selections[3] = new NamedExpression(buildColumn("sum"), buildColumn("sum"));
    }
    Project project = new Project(selections);
    project.setInput(collapsingAggregate);
    logicalOperators.add(project);

    // Output
    Store store = getStore();
    store.setInput(project);
    logicalOperators.add(store);
    Map<String, StorageEngineConfig> storageEngineMap = new HashMap<String, StorageEngineConfig>();
    return new LogicalPlan(buildPlanProperties(projectId), storageEngineMap, logicalOperators);
  }

}
