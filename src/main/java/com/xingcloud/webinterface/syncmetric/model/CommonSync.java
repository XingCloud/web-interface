package com.xingcloud.webinterface.syncmetric.model;

import com.xingcloud.webinterface.enums.Interval;

public class CommonSync extends AbstractSync {

  private Interval interval;

  public CommonSync() {
    super();
  }

  public CommonSync(String projectId, String event, String segment, Integer coverRangeOrigin, Integer coverRange,
                    Integer length, Interval interval) {
    super(projectId, event, segment, coverRangeOrigin, coverRange, length);
    this.interval = interval;
  }

  public Interval getInterval() {
    return interval;
  }

  public void setInterval(Interval interval) {
    this.interval = interval;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
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
    CommonSync other = (CommonSync) obj;
    if (interval != other.interval)
      return false;
    return true;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("CommonSync.");
    sb.append(projectId);
    sb.append('.');
    sb.append(event);
    sb.append('.');
    sb.append(segment);
    sb.append('.');
    sb.append(interval);
    sb.append("(CR.");
    sb.append(coverRange);
    sb.append(".CRO.");
    sb.append(coverRangeOrigin);
    sb.append(")#");
    sb.append(length);
    return sb.toString();
  }

}
