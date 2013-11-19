package com.xingcloud.webinterface.sql.desc;

import org.apache.drill.common.logical.data.Join;

/**
 * User: Z J Wu Date: 13-11-19 Time: 上午10:45 Package: com.xingcloud.webinterface.sql.desc
 */
public class JD extends SqlDescriptor {
  private SqlDescriptor leftSqlDescriptor;
  private SqlDescriptor rightSqlDescriptor;
  private Join.JoinType joinType;

  public JD(SqlDescriptor leftSqlDescriptor, SqlDescriptor rightSqlDescriptor, Join.JoinType joinType) {
    this.leftSqlDescriptor = leftSqlDescriptor;
    this.rightSqlDescriptor = rightSqlDescriptor;
    this.joinType = joinType;
  }
}
