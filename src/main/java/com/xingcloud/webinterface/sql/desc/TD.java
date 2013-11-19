package com.xingcloud.webinterface.sql.desc;

import net.sf.jsqlparser.expression.Expression;

/**
 * User: Z J Wu Date: 13-11-19 Time: 上午10:44 Package: com.xingcloud.webinterface.sql.desc
 */
public class TD extends SqlDescriptor {
  private Expression mainExpression;

  public TD() {
  }

  public TD(Expression mainExpression) {
    this.mainExpression = mainExpression;
  }
}
