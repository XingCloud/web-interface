package com.xingcloud.webinterface.enums;

/**
 * User: Z J Wu Date: 13-12-16 Time: 下午4:38 Package: com.xingcloud.webinterface.enums
 */
public enum MathOperation {
  A("Addition", "+"), S("Subtraction", "-"), M("Multiplication", "*"), D("Division", "/");

  private String name;

  private String expr;

  private MathOperation(String name, String expr) {
    this.name = name;
    this.expr = expr;
  }

  public String getName() {
    return name;
  }

  public String getExpr() {
    return expr;
  }
}
