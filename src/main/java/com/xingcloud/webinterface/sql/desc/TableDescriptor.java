package com.xingcloud.webinterface.sql.desc;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import com.xingcloud.webinterface.enums.Operator;
import com.xingcloud.webinterface.enums.SegmentTableType;
import com.xingcloud.webinterface.utils.WebInterfaceConstants;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * User: Z J Wu Date: 13-11-6 Time: 上午10:37 Package: com.xingcloud.webinterface.sql
 */
public class TableDescriptor implements Comparable<TableDescriptor> {

  @Expose
  @SerializedName("t")
  private SegmentTableType type;
  @Expose
  @SerializedName("c")
  private Map<String, List<ConditionUnit>> conditionUnits;

  private Map<String, Map<Operator, Object>> whereClauseMap;

  private Map<String, Operator> functionalPropertiesMap;

  public static TableDescriptor create(Map<String, Map<Operator, Object>> whereClauseMap, SegmentTableType type) {
    TableDescriptor tableDescriptor = new TableDescriptor();
    tableDescriptor.whereClauseMap = whereClauseMap;
    tableDescriptor.type = type;
    tableDescriptor.conditionUnits = new TreeMap<String, List<ConditionUnit>>();
    List<ConditionUnit> list;
    String propertyName;
    Operator operator;
    for (Map.Entry<String, Map<Operator, Object>> entry1 : whereClauseMap.entrySet()) {
      propertyName = entry1.getKey();
      list = new ArrayList<ConditionUnit>();
      tableDescriptor.conditionUnits.put(propertyName, list);
      for (Map.Entry<Operator, Object> entry2 : entry1.getValue().entrySet()) {
        operator = entry2.getKey();
        if (operator.isFunctional()) {
          if (tableDescriptor.functionalPropertiesMap == null) {
            tableDescriptor.functionalPropertiesMap = new HashMap<String, Operator>(1);
          }
          tableDescriptor.functionalPropertiesMap.put(propertyName, operator);
        }
        list.add(new ConditionUnit(operator, entry2.getValue()));
      }
    }
    return tableDescriptor;
  }

  public Map<String, Map<Operator, Object>> getWhereClauseMap() {
    return whereClauseMap;
  }

  public SegmentTableType getType() {
    return type;
  }

  public void setType(SegmentTableType type) {
    this.type = type;
  }

  public Map<String, Operator> getFunctionalPropertiesMap() {
    return functionalPropertiesMap;
  }

  public Map<String, List<ConditionUnit>> getConditionUnits() {
    return conditionUnits;
  }

  public void setConditionUnits(Map<String, List<ConditionUnit>> conditionUnits) {
    this.conditionUnits = conditionUnits;
  }

  @Override public String toString() {
    return WebInterfaceConstants.DEFAULT_SQL_GSON_PLAIN.toJson(this);
  }

  @Override public int compareTo(TableDescriptor o) {
    SegmentTableType thatSTT = o.getType();
    if (this.type.equals(thatSTT)) {
      Map<String, List<ConditionUnit>> thatContent = o.getConditionUnits();
      int thisSize = this.conditionUnits.size(), thatSize = thatContent.size();
      if (thisSize == thatSize) {
        Map.Entry<String, List<ConditionUnit>> thisEntry = this.conditionUnits.entrySet().iterator().next();
        Map.Entry<String, List<ConditionUnit>> thatEntry = thatContent.entrySet().iterator().next();
        String thisKey = thisEntry.getKey();
        String thatKey = thatEntry.getKey();
        if (thisKey.equals(thatKey)) {
          List<ConditionUnit> thisList = thisEntry.getValue(), thatList = thatEntry.getValue();
          if (thisList.size() == thatList.size()) {
            ConditionUnit thisCu, thatCu;
            int v;
            for (int i = 0; i < thisList.size(); i++) {
              thisCu = thisList.get(i);
              thatCu = thatList.get(i);
              v = thisCu.compareTo(thatCu);
              if (v == 0) {
                continue;
              } else {
                return v;
              }
            }
            return 0;
          } else {
            return thisList.size() - thatList.size();
          }
        } else {
          return thisKey.compareTo(thatKey);
        }
      } else {
        return thisSize - thatSize;
      }
    } else {
      return this.type.compareTo(thatSTT);
    }
  }

  public boolean isEventTable() {
    return SegmentTableType.E.equals(this.type);
  }

  public boolean isUserTable() {
    return !isEventTable();
  }

  public static void main(String[] args) {
    Set<ConditionUnit> s1 = new HashSet<>();
    Set<ConditionUnit> s2 = new HashSet<>();
    s1.add(new ConditionUnit(Operator.EQ,"a"));
    s2.add(new ConditionUnit(Operator.EQ,"a"));

    System.out.println(CollectionUtils.isEqualCollection(s1,s2));

  }
}
