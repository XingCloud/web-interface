package com.xingcloud.webinterface.sql.desc;

import static com.xingcloud.webinterface.enums.Operator.EQ;
import static com.xingcloud.webinterface.enums.Operator.GE;
import static com.xingcloud.webinterface.enums.Operator.GT;
import static com.xingcloud.webinterface.enums.Operator.LE;
import static com.xingcloud.webinterface.enums.Operator.LT;
import static com.xingcloud.webinterface.enums.SegmentTableType.E;
import static com.xingcloud.webinterface.plan.Plans.KEY_WORD_TIMESTAMP;
import static com.xingcloud.webinterface.plan.Plans.getChainedMysqlSegmentScan2;
import static com.xingcloud.webinterface.plan.Plans.getEventScan;
import static com.xingcloud.webinterface.plan.PlansNew.buildUIDAntiJoin;
import static com.xingcloud.webinterface.plan.PlansNew.buildUIDInnerJoin;
import static com.xingcloud.webinterface.sql.SqlUtilsConstants.DATE_FIELD;
import static com.xingcloud.webinterface.sql.SqlUtilsConstants.EVENT_FIELD;
import static com.xingcloud.webinterface.sql.SqlUtilsConstants.EVENT_TABLE_PREFIX;
import static com.xingcloud.webinterface.sql.SqlUtilsConstants.SEGMENT_TOSTRING_BEGIN;
import static com.xingcloud.webinterface.sql.SqlUtilsConstants.SEGMENT_TOSTRING_END;
import static com.xingcloud.webinterface.sql.SqlUtilsConstants.SQL_CONDITION_SEPARATOR;
import static com.xingcloud.webinterface.sql.SqlUtilsConstants.USER_TABLE_PREFIX;

import com.xingcloud.webinterface.enums.Operator;
import com.xingcloud.webinterface.enums.SegmentTableType;
import com.xingcloud.webinterface.exception.PlanException;
import com.xingcloud.webinterface.exception.SegmentException;
import com.xingcloud.webinterface.model.formula.CommonFormulaQueryDescriptor;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import org.apache.commons.collections.MapUtils;
import org.apache.drill.common.logical.data.Join;
import org.apache.drill.common.logical.data.LogicalOperator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * User: Z J Wu Date: 13-11-7 Time: 下午2:21 Package: com.xingcloud.webinterface.sql
 */
public class SegmentDescriptor {

  private FormulaQueryDescriptor descriptor;

  private Set<JoinDescriptor> joins;

  private List<TableDescriptor> event;

  private TableDescriptor user;

  private Map<String, Operator> functionalPropertiesMap;

  private LogicalOperator rootSegmentLogicalOperator;

  private List<LogicalOperator> logicalOperators;

  public SegmentDescriptor(FormulaQueryDescriptor descriptor) {
    this.descriptor = descriptor;
    this.logicalOperators = new ArrayList<LogicalOperator>(5);
  }

  public LogicalOperator getRootSegmentLogicalOperator() {
    return rootSegmentLogicalOperator;
  }

  public List<LogicalOperator> getLogicalOperators() {
    return logicalOperators;
  }

  public Map<String, Operator> getFunctionalPropertiesMap() {
    return functionalPropertiesMap;
  }

  public void addDescriptor(JoinDescriptor join) {
    Join.JoinType joinType = join.getJoinType();
    if (Join.JoinType.INNER.equals(joinType)) {
      addDescriptor(join.getLeft());
      addDescriptor(join.getRight());
      return;
    }
    if (joins == null) {
      joins = new TreeSet<>();
    }
    joins.add(join);
  }

  public void addDescriptor(TableDescriptor td) {
    if (E.equals(td.getType())) {
      addEventDescriptor(td);
    } else {
      addUserDescriptor(td);
    }
  }

  private void addUserDescriptor(TableDescriptor td) {
    if (this.user == null) {
      this.user = td;
      Map<String, Operator> tdFunctionalPropertiesMap = td.getFunctionalPropertiesMap();
      if (MapUtils.isEmpty(tdFunctionalPropertiesMap)) {
        return;
      }
      if (this.functionalPropertiesMap == null) {
        functionalPropertiesMap = new HashMap<>(1);
      }
      functionalPropertiesMap.putAll(tdFunctionalPropertiesMap);
      return;
    }

    String fieldName;
    Map<String, Map<Operator, Object>> thisWhereClause = this.user.getWhereClauseMap();
    Map<String, Map<Operator, Object>> whereClause = td.getWhereClauseMap();
    Map<Operator, Object> map;
    for (Map.Entry<String, Map<Operator, Object>> entry : whereClause.entrySet()) {
      fieldName = entry.getKey();
      map = thisWhereClause.get(fieldName);
      if (map == null) {
        map = new TreeMap<>();
        thisWhereClause.put(fieldName, map);
      }
      map.putAll(entry.getValue());
    }

    Map<String, List<ConditionUnit>> conditionUnits = this.user.getConditionUnits();
    Map<String, List<ConditionUnit>> cu = td.getConditionUnits();
    List<ConditionUnit> units;
    for (Map.Entry<String, List<ConditionUnit>> entry : cu.entrySet()) {
      fieldName = entry.getKey();
      units = conditionUnits.get(fieldName);
      if (units == null) {
        units = entry.getValue();
        conditionUnits.put(fieldName, units);
        continue;
      }
      units.addAll(entry.getValue());
    }
    Map<String, Operator> tdFunctionalPropertiesMap = td.getFunctionalPropertiesMap();
    if (MapUtils.isEmpty(tdFunctionalPropertiesMap)) {
      return;
    }
    if (this.functionalPropertiesMap == null) {
      functionalPropertiesMap = new HashMap<>(1);
    }
    functionalPropertiesMap.putAll(tdFunctionalPropertiesMap);
  }

  private void addEventDescriptor(TableDescriptor td) {
    if (this.event == null) {
      this.event = new ArrayList<>(3);
    }
    for (TableDescriptor tableDescriptor : this.event) {
      if (tableDescriptor.compareTo(td) == 0) {
        return;
      }
    }
    event.add(td);
  }

  public String toSegment() throws SegmentException {
    try {
      return _toSegment();
    } catch (SegmentException e) {
      throw e;
    } catch (Exception e) {
      throw new SegmentException(e);
    }
  }

  public String _toSegment() throws Exception {
    LogicalOperator lo1 = null, lo2;
    StringBuilder sb = new StringBuilder(SEGMENT_TOSTRING_BEGIN);

    boolean hasJoin = true;
    if (this.joins != null) {
      lo1 = appendAllJoins(sb);
    } else {
      hasJoin = false;
    }

    boolean hasEvent = true;
    if (this.event != null) {
      if (hasJoin) {
        sb.append(SQL_CONDITION_SEPARATOR);
      }
      lo2 = appendAllEvents(sb);
      if (lo1 == null) {
        lo1 = lo2;
      } else {
        lo1 = buildUIDInnerJoin(lo1, lo2);
        this.logicalOperators.add(lo1);
      }
    } else {
      hasEvent = false;
    }

    if (this.user != null) {
      if (hasEvent) {
        sb.append(SQL_CONDITION_SEPARATOR);
      }
      lo2 = appendAllUsers(sb);
      if (lo1 == null) {
        lo1 = lo2;
      } else {
        lo1 = buildUIDInnerJoin(lo1, lo2);
        this.logicalOperators.add(lo1);
      }
    }
    sb.append(SEGMENT_TOSTRING_END);
    this.rootSegmentLogicalOperator = lo1;
    return sb.toString();
  }

  private LogicalOperator makeOneEventTableLO(Map<String, Map<Operator, Object>> whereClauseMap) throws PlanException {
    // 额外的投影
    String[] additionalProjections;
    if (descriptor.isCommon()) {
      CommonFormulaQueryDescriptor cfqd = (CommonFormulaQueryDescriptor) descriptor;
      boolean min5HourQuery = cfqd.getInterval().getDays() < 1;
      additionalProjections = min5HourQuery ? new String[]{KEY_WORD_TIMESTAMP} : null;
    } else {
      additionalProjections = null;
    }
    // 拆分日期
    String beginDate, endDate;
    Map<Operator, Object> conditionMap = whereClauseMap.get(DATE_FIELD);
    Object val1 = conditionMap.get(EQ), val2;
    if (val1 == null) {
      val1 = conditionMap.get(GE);
      if (val1 == null) {
        val1 = conditionMap.get(GT);
        if (val1 == null) {
          throw new PlanException("Cannot parse lower bound in event table.");
        }
      }
      val2 = conditionMap.get(LE);
      if (val2 == null) {
        val2 = conditionMap.get(LT);
        if (val2 == null) {
          throw new PlanException("Cannot parse upper bound in event table.");
        }
      }
      endDate = val2.toString();
    } else {
      endDate = val1.toString();
    }
    beginDate = val1.toString();

    // 获取事件信息
    conditionMap = whereClauseMap.get(EVENT_FIELD);
    if (MapUtils.isEmpty(conditionMap)) {
      throw new PlanException("Event field must be assigned in event-table-sql.");
    }
    Object eventObj = conditionMap.get(EQ);
    if (eventObj == null) {
      throw new PlanException("Event operator must be EQ in event-table-sql - " + conditionMap);
    }
    LogicalOperator lo = getEventScan(descriptor.getProjectId(), eventObj.toString(), beginDate, endDate, false,
                                      additionalProjections);
    this.logicalOperators.add(lo);
    return lo;
  }

  // 由于以前的设计, logicalOperators.add(lo)这个操作已经在方法 getChainedMysqlSegmentScan2 里面了, 这里不用显示调用
  private LogicalOperator makeOneUserTableLO(Map<String, Map<Operator, Object>> whereClauseMap) throws PlanException {
    return getChainedMysqlSegmentScan2(this.descriptor.getProjectId(), this.logicalOperators, whereClauseMap);
  }

  private LogicalOperator makeOneJoinLO(JoinDescriptor jd) throws PlanException {
    LogicalOperator lo;
    Join.JoinType joinType = jd.getJoinType();
    TableDescriptor leftTD = jd.getLeft(), rightTD = jd.getRight();
    Map<String, Map<Operator, Object>> leftWhereClauseMap = leftTD.getWhereClauseMap();
    Map<String, Map<Operator, Object>> rightWhereClauseMap = rightTD.getWhereClauseMap();
    LogicalOperator leftLO =
      E.equals(leftTD.getType()) ? makeOneEventTableLO(leftWhereClauseMap) : makeOneUserTableLO(leftWhereClauseMap);
    LogicalOperator rightLO =
      E.equals(rightTD.getType()) ? makeOneEventTableLO(rightWhereClauseMap) : makeOneUserTableLO(rightWhereClauseMap);
    switch (joinType) {
      case INNER:
        lo = buildUIDInnerJoin(leftLO, rightLO);
        break;
      case ANTI:
        lo = buildUIDAntiJoin(leftLO, rightLO);
        break;
      default:
        throw new PlanException("Join type does not supported in segment-join - " + joinType);
    }
    this.logicalOperators.add(lo);
    return lo;
  }

  private LogicalOperator appendAllJoins(StringBuilder sb) throws PlanException {
    int s1 = joins.size(), i = 0;
    LogicalOperator lo1 = null, lo2;
    for (JoinDescriptor jd : joins) {
      ++i;
      appendOneJoinDescriptorString(sb, jd);
      if (!(i >= s1)) {
        sb.append(SQL_CONDITION_SEPARATOR);
      }
      lo2 = makeOneJoinLO(jd);
      if (lo1 == null) {
        lo1 = lo2;
      } else {
        lo1 = buildUIDInnerJoin(lo1, lo2);
        logicalOperators.add(lo1);
      }
    }

    return lo1;
  }

  private LogicalOperator appendAllUsers(StringBuilder sb) throws PlanException {
    appendOneTableDescriptorString(sb, this.user);
    return makeOneUserTableLO(this.user.getWhereClauseMap());
  }

  private LogicalOperator appendAllEvents(StringBuilder sb) throws PlanException {
    Collections.sort(this.event);
    int s1 = event.size(), i = 0;
    LogicalOperator lo1 = null, lo2;
    for (TableDescriptor td : event) {
      ++i;
      appendOneTableDescriptorString(sb, td);
      if (!(i >= s1)) {
        sb.append(SQL_CONDITION_SEPARATOR);
      }
      lo2 = makeOneEventTableLO(td.getWhereClauseMap());
      if (lo1 == null) {
        lo1 = lo2;
      } else {
        lo1 = buildUIDInnerJoin(lo1, lo2);
        logicalOperators.add(lo1);
      }
    }
    return lo1;
  }

  private void appendOneJoinDescriptorString(StringBuilder sb, JoinDescriptor joinDescriptor) {
    sb.append(joinDescriptor.getJoinType());
    sb.append('(');
    appendOneTableDescriptorString(sb, joinDescriptor.getLeft());
    sb.append('|');
    appendOneTableDescriptorString(sb, joinDescriptor.getRight());
    sb.append(')');
  }

  private void appendOneTableDescriptorString(StringBuilder sb, TableDescriptor td) {
    String fieldName;
    Operator operator;
    List<ConditionUnit> conditionUnits;
    SegmentTableType stt = td.getType();
    Map<String, List<ConditionUnit>> cus = td.getConditionUnits();
    String prefix = E.equals(stt) ? EVENT_TABLE_PREFIX : USER_TABLE_PREFIX;
    int size1 = cus.size(), size2, i = 0, j;
    for (Map.Entry<String, List<ConditionUnit>> entry : cus.entrySet()) {
      ++i;
      fieldName = entry.getKey();
      conditionUnits = entry.getValue();
      Collections.sort(conditionUnits);
      size2 = conditionUnits.size();
      j = 0;
      for (ConditionUnit cu : conditionUnits) {
        ++j;
        sb.append(prefix);
        sb.append(fieldName);
        operator = cu.getOperator();
        if (operator.needWhiteSpaceInToString()) {
          sb.append(' ');
          sb.append(cu.getOperator().getSqlOperator());
          sb.append(' ');
        } else {
          sb.append(cu.getOperator().getSqlOperator());
        }
        sb.append(object2String(cu.getValueObject()));
        if (!(i >= size1 && j >= size2)) {
          sb.append(SQL_CONDITION_SEPARATOR);
        }
      }
    }
  }

  private String object2String(Object valueObject) {
    if (valueObject instanceof String) {
      return "'" + valueObject.toString() + "'";
    } else if (valueObject instanceof Collection) {
      Iterator it = ((Collection) valueObject).iterator();
      StringBuilder sb = new StringBuilder("[");
      sb.append(object2String(it.next()));
      for (; ; ) {
        if (!it.hasNext()) {
          break;
        }
        sb.append(',');
        sb.append(object2String(it.next()));
      }
      sb.append("]");
      return sb.toString();
    } else {
      return valueObject.toString();
    }
  }

  public Set<JoinDescriptor> getJoins() {
    return joins;
  }

  public List<TableDescriptor> getEvent() {
    return event;
  }

  public TableDescriptor getUser() {
    return user;
  }

}
