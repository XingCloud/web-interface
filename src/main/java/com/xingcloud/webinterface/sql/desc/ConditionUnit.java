package com.xingcloud.webinterface.sql.desc;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import com.xingcloud.webinterface.enums.Operator;

/**
 * User: Z J Wu Date: 13-11-7 Time: 上午11:23 Package: com.xingcloud.webinterface.sql
 */
public class ConditionUnit implements Comparable<ConditionUnit> {
  @Expose
  @SerializedName("op")
  private Operator operator;
  @Expose
  @SerializedName("vo")
  private Object valueObject;

  public ConditionUnit(Operator operator, Object valueObject) {
    this.operator = operator;
    this.valueObject = valueObject;
  }

  public Operator getOperator() {
    return operator;
  }

  public Object getValueObject() {
    return valueObject;
  }

  @Override
  public String toString() {
    return this.operator.name() + this.valueObject.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ConditionUnit)) {
      return false;
    }

    ConditionUnit that = (ConditionUnit) o;

    if (operator != that.operator) {
      return false;
    }
    if (!valueObject.equals(that.valueObject)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = operator.hashCode();
    result = 31 * result + valueObject.hashCode();
    return result;
  }

  @Override
  public int compareTo(ConditionUnit o) {
    if (this.getOperator().equals(o.getOperator())) {
      Object vo1 = this.getValueObject(), vo2 = o.getValueObject();
      boolean b1 = vo1 instanceof Number, b2 = vo2 instanceof Number;
      if (b1 && b2) {
        return (int) (((Number) vo1).doubleValue() - ((Number) vo2).doubleValue());
      } else if (b1 && !b2) {
        return -1;
      } else if (!b1 && b2) {
        return 1;
      } else {
        return vo1.toString().compareTo(vo2.toString());
      }
    } else {
      return this.operator.compareTo(o.getOperator());
    }
  }
}
