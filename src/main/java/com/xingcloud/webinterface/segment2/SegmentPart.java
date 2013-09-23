package com.xingcloud.webinterface.segment2;

import java.io.Serializable;
import java.util.List;

/**
 * User: Z J Wu Date: 13-5-22 Time: 下午2:31 Package: com.xingcloud.webinterface.segment2
 */
public class SegmentPart implements Serializable, Comparable<SegmentPart> {
  private final String property;

  private List<Condition> conditions;

  public SegmentPart(String property, List<Condition> conditions) {
    this.property = property;
    this.conditions = conditions;
  }

  public String getProperty() {
    return property;
  }

  public List<Condition> getConditions() {
    return conditions;
  }

  public void setConditions(List<Condition> conditions) {
    this.conditions = conditions;
  }

  @Override
  public int compareTo(SegmentPart o) {
    return this.property.compareTo(o.getProperty());
  }

}
