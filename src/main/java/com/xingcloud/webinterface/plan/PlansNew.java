package com.xingcloud.webinterface.plan;

import static com.xingcloud.webinterface.plan.Plans.buildUidEQJoinCondition;

import org.apache.drill.common.logical.data.Join;
import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.drill.common.logical.data.LogicalOperator;

/**
 * User: Z J Wu Date: 13-11-13 Time: 上午11:53 Package: com.xingcloud.webinterface.plan
 */
public class PlansNew {

  public static LogicalOperator buildUIDInnerJoin(LogicalOperator left, LogicalOperator right) {
    return new Join(left, right, new JoinCondition[]{buildUidEQJoinCondition()}, Join.JoinType.INNER);
  }

  public static LogicalOperator buildUIDAntiJoin(LogicalOperator left, LogicalOperator right) {
    return new Join(left, right, new JoinCondition[]{buildUidEQJoinCondition()}, Join.JoinType.ANTI);
  }
}
