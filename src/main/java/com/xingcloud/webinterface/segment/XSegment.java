package com.xingcloud.webinterface.segment;

import com.xingcloud.webinterface.enums.Operator;
import org.apache.drill.common.logical.data.LogicalOperator;

import java.util.List;
import java.util.Map;

/**
 * User: Z J Wu Date: 13-11-8 Time: 上午11:17 Package: com.xingcloud.webinterface.segment
 */
public class XSegment {

  private String identifier;

  private LogicalOperator segmentLogicalOperator;

  private List<LogicalOperator> logicalOperators;

  private Map<String, Operator> functionalPropertiesMap;

  public XSegment(String identifier, LogicalOperator segmentLogicalOperator, List<LogicalOperator> logicalOperators,
                  Map<String, Operator> functionalPropertiesMap) {
    this.identifier = identifier;
    this.segmentLogicalOperator = segmentLogicalOperator;
    this.logicalOperators = logicalOperators;
    this.functionalPropertiesMap = functionalPropertiesMap;
  }

  public Map<String, Operator> getFunctionalPropertiesMap() {
    return functionalPropertiesMap;
  }

  public String getIdentifier() {
    return identifier;
  }

  public LogicalOperator getSegmentLogicalOperator() {
    return segmentLogicalOperator;
  }

  public List<LogicalOperator> getLogicalOperators() {
    return logicalOperators;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof XSegment)) {
      return false;
    }

    XSegment segment = (XSegment) o;

    if (identifier != null ? !identifier.equals(segment.identifier) : segment.identifier != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return identifier != null ? identifier.hashCode() : 0;
  }
}
