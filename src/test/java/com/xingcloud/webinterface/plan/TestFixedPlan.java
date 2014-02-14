package com.xingcloud.webinterface.plan;

import com.xingcloud.webinterface.enums.CommonQueryType;
import com.xingcloud.webinterface.enums.GroupByType;
import com.xingcloud.webinterface.enums.Interval;
import com.xingcloud.webinterface.model.Filter;
import com.xingcloud.webinterface.model.formula.CommonFormulaQueryDescriptor;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import com.xingcloud.webinterface.model.formula.GroupByFormulaQueryDescriptor;
import com.xingcloud.webinterface.segment.SegmentEvaluator;
import org.apache.drill.common.logical.LogicalPlan;
import org.junit.Test;

/**
 * User: Z J Wu Date: 13-9-22 Time: 下午4:16 Package: com.xingcloud.webinterface.plan
 */
public class TestFixedPlan extends TestLogicalPlanBase {

  @Test
  public void test() throws Exception {
    String projectId = "xaa";
    String e1 = "heartbeat.*", e2 = "visit.*", e3 = "response.*.*.*.show.*",
      e4 = "response.*.*.*.pend.*", e5 = "response.*.*.*.responsetime.*", e6 = "response.hayday.*.*.pend.*";
    String d1 = "2013-09-21", d2 = "2013-09-22";
    FormulaQueryDescriptor[] fqds = new FormulaQueryDescriptor[7];
    fqds[0] = new CommonFormulaQueryDescriptor(projectId, d1, d1, e1, null, Filter.ALL, d1, d1, Interval.HOUR,
                                               CommonQueryType.NORMAL);
    fqds[1] = new CommonFormulaQueryDescriptor(projectId, d2, d2, e2, null, Filter.ALL, d2, d2, Interval.HOUR,
                                               CommonQueryType.NORMAL);
    fqds[2] = new CommonFormulaQueryDescriptor(projectId, d2, d2, e1, null, Filter.ALL, d2, d2, Interval.HOUR,
                                               CommonQueryType.NORMAL);
    fqds[3] = new CommonFormulaQueryDescriptor(projectId, d1, d1, e2, null, Filter.ALL, d1, d1, Interval.HOUR,
                                               CommonQueryType.NORMAL);
    fqds[4] = new GroupByFormulaQueryDescriptor(projectId, d2, d2, e3, null, Filter.ALL, "1", GroupByType.EVENT);
    fqds[5] = new GroupByFormulaQueryDescriptor(projectId, d2, d2, e4, null, Filter.ALL, "1", GroupByType.EVENT);
    fqds[6] = new GroupByFormulaQueryDescriptor(projectId, d2, d2, e5, null, Filter.ALL, "1", GroupByType.EVENT);

    String name = "fixedplan";
    String planString;
    LogicalPlan logicalPlan;
    for (int i = 0; i < fqds.length; i++) {
      SegmentEvaluator.evaluate(fqds[i]);
      logicalPlan = fqds[i].toLogicalPlain();
      planString = Plans.DEFAULT_DRILL_CONFIG.getMapper().writeValueAsString(logicalPlan);
      write2File(name + "." + i + ".json", planString);
    }

    FormulaQueryDescriptor fqd = new CommonFormulaQueryDescriptor(projectId, d1, d1, "response.hayday.*.*.pend.*", null,
                                                                  Filter.ALL, d1, d1, Interval.PERIOD,
                                                                  CommonQueryType.NORMAL);
    SegmentEvaluator.evaluate(fqd);
    logicalPlan = fqd.toLogicalPlain();
    planString = Plans.DEFAULT_DRILL_CONFIG.getMapper().writeValueAsString(logicalPlan);
    System.out.println(planString);

  }
}
