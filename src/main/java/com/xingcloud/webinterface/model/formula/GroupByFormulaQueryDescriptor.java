package com.xingcloud.webinterface.model.formula;

import static com.xingcloud.basic.Constants.SEPARATOR_CHAR_CACHE;
import static com.xingcloud.webinterface.enums.GroupByType.EVENT_VAL;
import static com.xingcloud.webinterface.enums.GroupByType.USER_PROPERTIES;
import static com.xingcloud.webinterface.plan.Plans.DFR;
import static com.xingcloud.webinterface.plan.Plans.KEY_WORD_DIMENSION;
import static com.xingcloud.webinterface.plan.Plans.KEY_WORD_EVENT;
import static com.xingcloud.webinterface.plan.Plans.KEY_WORD_SGMT;
import static com.xingcloud.webinterface.plan.Plans.KEY_WORD_UID;
import static com.xingcloud.webinterface.plan.Plans.KEY_WORD_VALUE;
import static com.xingcloud.webinterface.plan.Plans.buildPlanProperties;
import static com.xingcloud.webinterface.plan.Plans.getChainedMysqlSegmentScan;
import static com.xingcloud.webinterface.plan.Plans.getEventScan;
import static com.xingcloud.webinterface.plan.Plans.getStore;
import static com.xingcloud.webinterface.plan.Plans.getUserScan;
import static org.apache.drill.common.enums.Aggregator.COUNT;
import static org.apache.drill.common.enums.Aggregator.COUNT_DISTINCT;
import static org.apache.drill.common.enums.Aggregator.SUM;
import static org.apache.drill.common.util.FieldReferenceBuilder.buildColumn;

import com.xingcloud.mysql.PropType;
import com.xingcloud.mysql.UserProp;
import com.xingcloud.webinterface.enums.GroupByType;
import com.xingcloud.webinterface.enums.Operator;
import com.xingcloud.webinterface.exception.PlanException;
import com.xingcloud.webinterface.model.Filter;
import com.xingcloud.webinterface.utils.UserPropertiesInfoManager;
import org.apache.commons.lang3.StringUtils;
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

public class GroupByFormulaQueryDescriptor extends FormulaQueryDescriptor {

  private String groupBy;

  private GroupByType groupByType;

  public GroupByFormulaQueryDescriptor() {
    super();
  }

  public GroupByFormulaQueryDescriptor(String projectId, String realBeginDate, String realEndDate, String event,
                                       String segment, Filter filter, double samplingRate, GroupByType groupByType) {
    super(projectId, realBeginDate, realEndDate, event, segment, filter, samplingRate);
    this.groupByType = groupByType;
  }

  public GroupByFormulaQueryDescriptor(String projectId, String realBeginDate, String realEndDate, String event,
                                       String segment, Filter filter, double samplingRate, String inputBeginDate,
                                       String inputEndDate, GroupByType groupByType) {
    super(projectId, realBeginDate, realEndDate, event, segment, filter, samplingRate, inputBeginDate, inputEndDate);
    this.groupByType = groupByType;
  }

  public GroupByFormulaQueryDescriptor(String projectId, String realBeginDate, String realEndDate, String event,
                                       String segment, Filter filter, double samplingRate, String groupBy,
                                       GroupByType groupByType) {
    super(projectId, realBeginDate, realEndDate, event, segment, filter, samplingRate);
    this.groupBy = groupBy;
    this.groupByType = groupByType;
  }

  public GroupByFormulaQueryDescriptor(String projectId, String realBeginDate, String realEndDate, String event,
                                       String segment, Filter filter, double samplingRate, String inputBeginDate,
                                       String inputEndDate, String groupBy, GroupByType groupByType) {
    super(projectId, realBeginDate, realEndDate, event, segment, filter, samplingRate, inputBeginDate, inputEndDate);
    this.groupBy = groupBy;
    this.groupByType = groupByType;
  }

  public String toKey() {
    return toKey(true);
  }

  public GroupByFormulaQueryDescriptor(String groupBy, GroupByType groupByType) {
    super();
    this.groupBy = groupBy;
    this.groupByType = groupByType;
  }

  public String toKey(boolean ignore) {
    StringBuilder sb = toKeyGeneric();
    if (sb == null) {
      return null;
    }

    sb.append(SEPARATOR_CHAR_CACHE);
    sb.append(getGroupByType());
    if (StringUtils.isNotBlank(getGroupBy())) {
      sb.append(SEPARATOR_CHAR_CACHE);
      sb.append(getGroupBy());
    }

    if (!ignore) {
      toStringGeneric(sb);
    }
    return sb.toString();
  }

  public String getGroupBy() {
    return groupBy;
  }

  public void setGroupBy(String groupBy) {
    this.groupBy = groupBy;
  }

  public GroupByType getGroupByType() {
    return groupByType;
  }

  public void setGroupByType(GroupByType groupByType) {
    this.groupByType = groupByType;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((groupBy == null) ? 0 : groupBy.hashCode());
    result = prime * result + ((groupByType == null) ? 0 : groupByType.hashCode()
    );
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
    GroupByFormulaQueryDescriptor other = (GroupByFormulaQueryDescriptor) obj;
    if (groupBy == null) {
      if (other.groupBy != null)
        return false;
    } else if (!groupBy.equals(other.groupBy))
      return false;
    if (groupByType != other.groupByType)
      return false;
    return true;
  }

  @Override
  public String toString() {
    return toKey(false);
  }

  @Override
  public LogicalPlan toLogicalPlain() throws PlanException {
    List<LogicalOperator> logicalOperators = new ArrayList<LogicalOperator>();
    boolean userPropertyGroupBy = USER_PROPERTIES.equals(this.groupByType);
    boolean eventValGroupBy = EVENT_VAL.equals(this.getGroupByType());

    String[] additionalProjectionOfEventTable =
      (userPropertyGroupBy || eventValGroupBy) ? null : new String[]{KEY_WORD_EVENT + groupBy};

    LogicalOperator eventTableScan = getEventScan(this.projectId, this.event, this.realBeginDate, this.realEndDate,
                                                  additionalProjectionOfEventTable);
    logicalOperators.add(eventTableScan);

    Join join;
    JoinCondition[] joinConditions;
    LogicalOperator scanRoot, segmentLogicalOperator, userTableScan;

    FieldReference uidFR = buildColumn(KEY_WORD_UID);
    if (userPropertyGroupBy) {
      userTableScan = getUserScan(this.projectId, this.groupBy);
      logicalOperators.add(userTableScan);
      if (hasSegment()) {
        Map<String, Map<Operator, Object>> segmentMap = getSegmentMap();
        if (segmentMap.containsKey("N/A")) {
          return null;
        }
        segmentLogicalOperator = getChainedMysqlSegmentScan(this.projectId, logicalOperators, segmentMap);
        joinConditions = new JoinCondition[1];
        joinConditions[0] = new JoinCondition("==", uidFR, uidFR);
        join = new Join(segmentLogicalOperator, eventTableScan, joinConditions, Join.JoinType.INNER);
        logicalOperators.add(join);

        JoinCondition[] joinConditions2 = new JoinCondition[1];
        joinConditions2[0] = new JoinCondition("==", uidFR, uidFR);
        Join join2 = new Join(userTableScan, join, joinConditions2, Join.JoinType.RIGHT);
        logicalOperators.add(join2);
        scanRoot = join2;
      } else {
        joinConditions = new JoinCondition[1];
        joinConditions[0] = new JoinCondition("==", uidFR, uidFR);
        join = new Join(userTableScan, eventTableScan, joinConditions, Join.JoinType.RIGHT);
        logicalOperators.add(join);
        scanRoot = join;
      }
    } else {
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
    }

    String groupByExpr;
    FieldReference singleGroupByLE;
    if (userPropertyGroupBy) {
      UserProp up;
      try {
        up = UserPropertiesInfoManager.getInstance().getUserProp(this.projectId, this.groupBy);
      } catch (Exception e) {
        throw new PlanException(e);
      }
      PropType pt = up.getPropType();
      if (PropType.sql_datetime.equals(pt)) {
        groupByExpr = "date(" + groupBy + ")";
      } else {
        groupByExpr = groupBy;
      }
    } else if (eventValGroupBy) {
      groupByExpr = KEY_WORD_VALUE;
    } else {
      groupByExpr = KEY_WORD_EVENT + groupBy;
    }
    singleGroupByLE = buildColumn(groupByExpr);
    FieldReference fr = buildColumn(KEY_WORD_DIMENSION);
    FieldReference fr2 = buildColumn(KEY_WORD_SGMT);
    Segment segment = new Segment(new NamedExpression[]{new NamedExpression(singleGroupByLE, fr)}, fr2);
    segment.setInput(scanRoot);
    logicalOperators.add(segment);

    // Build collapsing aggregation
    CollapsingAggregate collapsingAggregate;
    FieldReference within = (segment == null ? null : segment.getName()), target = null;
    FieldReference[] carryovers = new FieldReference[1];
    NamedExpression[] namedExpressions = new NamedExpression[3];

    carryovers[0] = buildColumn(KEY_WORD_DIMENSION);

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
    collapsingAggregate.setInput(segment);
    logicalOperators.add(collapsingAggregate);

    String queryId = getKey();
    NamedExpression[] selections = new NamedExpression[5];
    LogicalExpression constQueryId = new ValueExpressions.QuotedString(queryId, ExpressionPosition.UNKNOWN);
    selections[0] = new NamedExpression(constQueryId, buildColumn("query_id"));
    selections[1] = new NamedExpression(buildColumn(KEY_WORD_DIMENSION), buildColumn(KEY_WORD_DIMENSION));
    selections[2] = new NamedExpression(buildColumn("count"), buildColumn("count"));
    selections[3] = new NamedExpression(buildColumn("user_num"), buildColumn("user_num"));
    selections[4] = new NamedExpression(buildColumn("sum"), buildColumn("sum"));
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
