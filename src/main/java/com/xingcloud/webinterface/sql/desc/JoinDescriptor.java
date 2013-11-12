package com.xingcloud.webinterface.sql.desc;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import com.xingcloud.webinterface.utils.WebInterfaceConstants;
import org.apache.drill.common.logical.data.Join;

/**
 * User: Z J Wu Date: 13-11-7 Time: 上午10:58 Package: com.xingcloud.webinterface.sql
 */
public class JoinDescriptor implements Comparable<JoinDescriptor> {

  @Expose
  @SerializedName("t")
  private Join.JoinType joinType;

  @Expose
  @SerializedName("l")
  private TableDescriptor left;

  @Expose
  @SerializedName("r")
  private TableDescriptor right;

  public JoinDescriptor(Join.JoinType joinType, TableDescriptor left, TableDescriptor right) {
    this.joinType = joinType;
    this.left = left;
    this.right = right;
  }

  public Join.JoinType getJoinType() {
    return joinType;
  }

  public void setJoinType(Join.JoinType joinType) {
    this.joinType = joinType;
  }

  public TableDescriptor getLeft() {
    return left;
  }

  public void setLeft(TableDescriptor left) {
    this.left = left;
  }

  public TableDescriptor getRight() {
    return right;
  }

  public void setRight(TableDescriptor right) {
    this.right = right;
  }

  @Override public String toString() {
    return WebInterfaceConstants.DEFAULT_SQL_GSON_PLAIN.toJson(this);
  }

  @Override public int compareTo(JoinDescriptor o) {
    if (this.joinType.equals(o.getJoinType())) {
      int v = this.left.compareTo(o.getLeft());
      if (v == 0) {
        return this.right.compareTo(o.getRight());
      } else {
        return v;
      }
    } else {
      return this.joinType.compareTo(o.getJoinType());
    }
  }
}
