package com.xingcloud.webinterface.plan;

import org.apache.drill.common.expression.LogicalExpression;

/**
 * User: Z J Wu Date: 13-9-17 Time: 下午3:38 Package: com.xingcloud.webinterface.plan
 */
public class ScanFilter {

  private LogicalExpression expression;

  public ScanFilter(LogicalExpression expression) {
    this.expression = expression;
  }

  public LogicalExpression getExpression() {
    return expression;
  }
}
