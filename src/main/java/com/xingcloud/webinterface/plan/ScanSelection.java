package com.xingcloud.webinterface.plan;

import org.apache.drill.common.logical.data.NamedExpression;

/**
 * User: Z J Wu Date: 13-9-17 Time: 下午3:38 Package: com.xingcloud.webinterface.plan
 */
public class ScanSelection {

  private String table;
  private ScanFilter filter;
  private NamedExpression[] projections;

  public ScanSelection(String table, ScanFilter filter, NamedExpression[] projections) {
    this.table = table;
    this.filter = filter;
    this.projections = projections;
  }

  public String getTable() {
    return table;
  }

  public ScanFilter getFilter() {
    return filter;
  }

  public NamedExpression[] getProjections() {
    return projections;
  }
}
