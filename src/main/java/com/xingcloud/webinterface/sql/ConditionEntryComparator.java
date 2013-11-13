package com.xingcloud.webinterface.sql;

import com.xingcloud.webinterface.enums.Operator;

import java.util.Comparator;
import java.util.Map;

/**
 * User: Z J Wu Date: 13-11-12 Time: 下午6:16 Package: com.xingcloud.webinterface.sql
 */
public class ConditionEntryComparator<T extends Map.Entry<Operator, Object>> implements Comparator<T> {



  @Override
  public int compare(T entry1, T entry2) {
    Operator op1 = entry1.getKey(), op2 = entry2.getKey();
    if (op1.equals(op2)) {
      Object v1 = entry1.getValue(), v2 = entry2.getValue();
      boolean b1 = v1 instanceof Number, b2 = v2 instanceof Number;
      if (b1 && b2) {
        return (int) (((Number) v1).doubleValue() - ((Number) v2).doubleValue());
      } else if (b1 && !b2) {
        return -1;
      } else if (!b1 && b2) {
        return 1;
      } else {
        return v1.toString().compareTo(v2.toString());
      }
    } else {
      return op1.compareTo(op2);
    }
  }
}
