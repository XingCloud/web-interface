package com.xingcloud.webinterface.syncmetric.model;

import com.google.common.base.Strings;
import com.xingcloud.webinterface.enums.Interval;
import com.xingcloud.webinterface.exception.RefelectException;
import com.xingcloud.webinterface.utils.ReflectUtils;
import com.xingcloud.webinterface.utils.WebInterfaceConstants;

import java.util.Map;

public abstract class AbstractSync {

  protected String projectId;

  protected String event;

  protected String segment;

  protected Integer coverRangeOrigin;

  protected Integer coverRange;

  protected Integer length;

  public AbstractSync() {
    super();
  }

  public AbstractSync(String projectId, String event, String segment, Integer coverRangeOrigin, Integer coverRange,
                      Integer length) {
    this.projectId = projectId;
    this.event = event;
    this.segment = segment;
    this.coverRangeOrigin = coverRangeOrigin;
    this.coverRange = coverRange;
    this.length = length;
  }

  public Map<String, Object> toMap() throws RefelectException {
    Class<?> superClass = this.getClass().getSuperclass();
    Map<String, Object> map = ReflectUtils.getDeclaredFieldNameValueMap(superClass, this);
    ReflectUtils.getDeclaredFieldNameValueMap(this.getClass(), this, map);
    return map;
  }

  public String getProjectId() {
    return projectId;
  }

  public void setProjectId(String projectId) {
    this.projectId = projectId;
  }

  public String getEvent() {
    return event;
  }

  public void setEvent(String event) {
    this.event = event;
  }

  public String getSegment() {
    return segment;
  }

  public void setSegment(String segment) {
    this.segment = segment;
  }

  public Integer getCoverRangeOrigin() {
    return coverRangeOrigin;
  }

  public void setCoverRangeOrigin(Integer coverRangeOrigin) {
    this.coverRangeOrigin = coverRangeOrigin;
  }

  public Integer getCoverRange() {
    return coverRange;
  }

  public void setCoverRange(Integer coverRange) {
    this.coverRange = coverRange;
  }

  public Integer getLength() {
    return length;
  }

  public void setLength(Integer length) {
    this.length = length;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((coverRange == null) ? 0 : coverRange.hashCode()
    );
    result = prime * result + ((coverRangeOrigin == null) ? 0 : coverRangeOrigin.hashCode()
    );
    result = prime * result + ((event == null) ? 0 : event.hashCode());
    result = prime * result + ((length == null) ? 0 : length.hashCode());
    result = prime * result + ((projectId == null) ? 0 : projectId.hashCode());
    result = prime * result + ((segment == null) ? 0 : segment.hashCode());
    return result;
  }

  public boolean isValid() {
    if (Strings.isNullOrEmpty(this.event)) {
      return false;
    }
    return true;
  }

  public void trim() {
    if (this.coverRange == null) {
      this.coverRange = 0;
    }
    if (this.coverRangeOrigin == null) {
      this.coverRangeOrigin = 0;
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    AbstractSync other = (AbstractSync) obj;
    if (coverRange == null) {
      if (other.coverRange != null)
        return false;
    } else if (!coverRange.equals(other.coverRange))
      return false;
    if (coverRangeOrigin == null) {
      if (other.coverRangeOrigin != null)
        return false;
    } else if (!coverRangeOrigin.equals(other.coverRangeOrigin))
      return false;
    if (event == null) {
      if (other.event != null)
        return false;
    } else if (!event.equals(other.event))
      return false;
    if (length == null) {
      if (other.length != null)
        return false;
    } else if (!length.equals(other.length))
      return false;
    if (projectId == null) {
      if (other.projectId != null)
        return false;
    } else if (!projectId.equals(other.projectId))
      return false;
    if (segment == null) {
      if (other.segment != null)
        return false;
    } else if (!segment.equals(other.segment))
      return false;
    return true;
  }

  public static void main(String[] args) throws RefelectException {
    // AbstractSync as2 = new GroupBySync("tencent-18894", "visit.*",
    // Constants.TOTAL_USER, 1, 2, 3, null);

    AbstractSync as1 = new CommonSync("tencent-18894", "visit.*", WebInterfaceConstants.TOTAL_USER, null, null, null,
                                      Interval.DAY);
    System.out.println(as1);
    System.out.println(as1.toMap());
  }
}
