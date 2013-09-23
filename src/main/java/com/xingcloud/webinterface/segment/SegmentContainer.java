package com.xingcloud.webinterface.segment;

import java.util.List;
import java.util.Map;

public class SegmentContainer {
  private Map<Object, Object> fixedSegmentMap;

  private Map<String, List<Object>> splittedSegmentPart;

  public SegmentContainer(Map<Object, Object> fixedSegmentMap) {
    super();
    this.fixedSegmentMap = fixedSegmentMap;
  }

  public SegmentContainer(Map<Object, Object> fixedSegmentMap, Map<String, List<Object>> splittedSegmentPart) {
    super();
    this.fixedSegmentMap = fixedSegmentMap;
    this.splittedSegmentPart = splittedSegmentPart;
  }

  public Map<Object, Object> getFixedSegmentMap() {
    return fixedSegmentMap;
  }

  public void setFixedSegmentMap(Map<Object, Object> fixedSegmentMap) {
    this.fixedSegmentMap = fixedSegmentMap;
  }

  public Map<String, List<Object>> getSplittedSegmentPart() {
    return splittedSegmentPart;
  }

  public void setSplittedSegmentPart(Map<String, List<Object>> splittedSegmentPart) {
    this.splittedSegmentPart = splittedSegmentPart;
  }

}
