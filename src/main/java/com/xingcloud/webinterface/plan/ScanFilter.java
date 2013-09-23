package com.xingcloud.webinterface.plan;

import org.apache.drill.common.expression.LogicalExpression;

/**
 * User: Z J Wu Date: 13-9-17 Time: 下午3:38 Package: com.xingcloud.webinterface.plan
 */
public class ScanFilter {

  private ScanFilterType type;
  private LogicalExpression expression;

  public ScanFilter(ScanFilterType type, LogicalExpression expression) {
    this.type = type;
    this.expression = expression;
  }

  public ScanFilterType getType() {
    return type;
  }

  public LogicalExpression getExpression() {
    return expression;
  }
}
