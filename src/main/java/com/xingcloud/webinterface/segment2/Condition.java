package com.xingcloud.webinterface.segment2;

import com.xingcloud.webinterface.enums.Operator;
import com.xingcloud.webinterface.enums.SegmentExprType;

import java.io.Serializable;

/**
 * User: Z J Wu Date: 13-5-22 Time: 下午2:32 Package: com.xingcloud.webinterface.segment2
 */
public class Condition implements Serializable {

  private Operator operator;

  private String expression;

  private SegmentExprType type;

  public Condition() {
  }

  public Condition(Operator operator, String expression, SegmentExprType type) {
    this.operator = operator;
    this.expression = expression;
    this.type = type;
  }

  public Operator getOperator() {
    return operator;
  }

  public void setOperator(Operator operator) {
    this.operator = operator;
  }

  public String getExpression() {
    return expression;
  }

  public void setExpression(String expression) {
    this.expression = expression;
  }

  public SegmentExprType getType() {
    return type;
  }

  public void setType(SegmentExprType type) {
    this.type = type;
  }

}
