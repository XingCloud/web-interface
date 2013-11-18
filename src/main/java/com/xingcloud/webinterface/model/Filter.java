package com.xingcloud.webinterface.model;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import com.xingcloud.webinterface.annotation.Ignore;
import com.xingcloud.webinterface.annotation.JsonName;
import com.xingcloud.webinterface.enums.Operator;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public class Filter implements Serializable {

  @Ignore
  private static final long serialVersionUID = -8231689763120240886L;

  @Ignore
  public static final Filter ALL = new Filter(Operator.ALL, 0, 0);

  @Expose
  @SerializedName("comparison_operator")
  @JsonName("comparison_operator")
  private Operator operator;
  @Expose
  @SerializedName("comparison_value")
  @JsonName("comparison_value")
  private long value;
  @Expose
  @SerializedName("comparison_value2")
  @JsonName("comparison_value2")
  private long extraValue;

  public Filter() {
    super();
  }

  public Filter(Operator operator) {
    super();
    this.operator = operator;
  }

  public Filter(Operator operator, long value, long extraValue) {
    super();
    this.operator = operator;
    this.value = value;
    this.extraValue = extraValue;
  }

  public Filter(Operator operator, long value) {
    super();
    this.operator = operator;
    this.value = value;
  }

  public Operator getOperator() {
    return operator;
  }

  public void setOperator(Operator operator) {
    this.operator = operator;
  }

  public long getValue() {
    return value;
  }

  public void setValue(long value) {
    this.value = value;
  }

  public long getExtraValue() {
    return extraValue;
  }

  public void setExtraValue(long extraValue) {
    this.extraValue = extraValue;
  }

  public String toString() {
    return "VF-" + operator + "-" + value + "-" + extraValue;
  }

  public void write(DataOutput out) throws IOException {
    WritableUtils.writeEnum(out, operator);
    WritableUtils.writeVLong(out, this.value);
    WritableUtils.writeVLong(out, this.extraValue);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (extraValue ^ (extraValue >>> 32));
    result = prime * result + ((operator == null) ? 0 : operator.hashCode());
    result = prime * result + (int) (value ^ (value >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Filter other = (Filter) obj;
    if (extraValue != other.extraValue)
      return false;
    if (operator != other.operator)
      return false;
    if (value != other.value)
      return false;
    return true;
  }
}
