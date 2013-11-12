package com.xingcloud.webinterface.sql.desc;

import static com.xingcloud.webinterface.enums.SegmentTableType.E;
import static com.xingcloud.webinterface.sql.SqlUtilsConstants.EVENT_TABLE_PREFIX;
import static com.xingcloud.webinterface.sql.SqlUtilsConstants.SEGMENT_TOSTRING_BEGIN;
import static com.xingcloud.webinterface.sql.SqlUtilsConstants.SEGMENT_TOSTRING_END;
import static com.xingcloud.webinterface.sql.SqlUtilsConstants.SQL_CONDITION_SEPARATOR;
import static com.xingcloud.webinterface.sql.SqlUtilsConstants.USER_TABLE_PREFIX;

import com.xingcloud.webinterface.enums.Operator;
import com.xingcloud.webinterface.enums.SegmentTableType;
import org.apache.commons.collections.MapUtils;
import org.apache.drill.common.logical.data.Join;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * User: Z J Wu Date: 13-11-7 Time: 下午2:21 Package: com.xingcloud.webinterface.sql
 */
public class SegmentDescriptor {
  private Set<JoinDescriptor> joins;

  private List<TableDescriptor> event;

  private TableDescriptor user;

  private Map<String, Operator> functionalPropertiesMap;

  public Map<String, Operator> getFunctionalPropertiesMap() {
    return functionalPropertiesMap;
  }

  public void addDescriptor(JoinDescriptor join) {
    if (joins == null) {
      joins = new TreeSet<JoinDescriptor>();
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
    Map<String, Operator> existFunctionalPropertiesMap = td.getFunctionalPropertiesMap();
    if (this.user == null) {
      this.user = td;
      if (MapUtils.isEmpty(existFunctionalPropertiesMap)) {
        return;
      }
      if (this.functionalPropertiesMap == null) {
        functionalPropertiesMap = new HashMap<String, Operator>(1);
      }
      functionalPropertiesMap.putAll(existFunctionalPropertiesMap);
      return;
    }
    Map<String, List<ConditionUnit>> conditionUnits = this.user.getConditionUnits();
    Map<String, List<ConditionUnit>> cu = td.getConditionUnits();
    String fieldName;
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
    if (MapUtils.isEmpty(existFunctionalPropertiesMap)) {
      return;
    }
    if (this.functionalPropertiesMap == null) {
      functionalPropertiesMap = new HashMap<String, Operator>(1);
    }
    functionalPropertiesMap.putAll(existFunctionalPropertiesMap);
  }

  private void addEventDescriptor(TableDescriptor td) {
    if (this.event == null) {
      this.event = new ArrayList<TableDescriptor>(3);
    }
    event.add(td);
  }

  public String toSegmentKey() {
    StringBuilder sb = new StringBuilder(SEGMENT_TOSTRING_BEGIN);
    boolean hasJoin = true;
    if (this.joins != null) {
      JoinDescriptor jd;
      Iterator<JoinDescriptor> it = joins.iterator();
      jd = it.next();
      appendJoin(sb, jd.getJoinType(), jd.getLeft(), jd.getRight());
      for (; ; ) {
        if (!it.hasNext()) {
          break;
        }
        sb.append(SQL_CONDITION_SEPARATOR);
        jd = it.next();
        appendJoin(sb, jd.getJoinType(), jd.getLeft(), jd.getRight());
      }
    } else {
      hasJoin = false;
    }

    boolean hasEvent = true;
    if (this.event != null) {
      if (hasJoin) {
        sb.append(SQL_CONDITION_SEPARATOR);
      }
      appendEvents(sb);
    } else {
      hasEvent = false;
    }

    if (this.user != null) {
      if (hasEvent) {
        sb.append(SQL_CONDITION_SEPARATOR);
      }
      appendSingleTableDescriptor(sb, user);
    }
    sb.append(SEGMENT_TOSTRING_END);
    return sb.toString();
  }

  private void appendEvents(StringBuilder sb) {
    Collections.sort(this.event);
    System.out.println(this.event);
    String fieldName;
    Operator operator;
    List<ConditionUnit> conditionUnits;
    Map<String, List<ConditionUnit>> cus;
    String prefix = EVENT_TABLE_PREFIX;
    int s1 = this.event.size(), s2, s3, i = 0, j, k;
    for (TableDescriptor td : this.event) {
      ++i;
      cus = td.getConditionUnits();
      s2 = cus.size();
      j = 0;
      for (Map.Entry<String, List<ConditionUnit>> entry : cus.entrySet()) {
        ++j;
        fieldName = entry.getKey();
        conditionUnits = entry.getValue();
        Collections.sort(conditionUnits);
        s3 = conditionUnits.size();
        k = 0;
        for (ConditionUnit cu : conditionUnits) {
          ++k;
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
          if (!(i >= s1 && j >= s2 && k >= s3)) {
            sb.append(SQL_CONDITION_SEPARATOR);
          }
        }
      }
    }
  }

  private void appendJoin(StringBuilder sb, Join.JoinType joinType, TableDescriptor left, TableDescriptor right) {
    sb.append(joinType);
    sb.append('(');
    appendSingleTableDescriptor(sb, left);
    sb.append('|');
    appendSingleTableDescriptor(sb, right);
    sb.append(')');
  }

  private void appendSingleTableDescriptor(StringBuilder sb, TableDescriptor td) {
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
