package com.xingcloud.webinterface.plan;

import com.xingcloud.qm.service.Submit;
import com.xingcloud.webinterface.enums.GroupByType;
import com.xingcloud.webinterface.model.Filter;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import com.xingcloud.webinterface.model.formula.GroupByFormulaQueryDescriptor;
import com.xingcloud.webinterface.segment.SegmentEvaluator;
import org.apache.drill.common.logical.LogicalPlan;
import org.junit.Test;

/**
 * User: Z J Wu Date: 13-8-6 Time: 下午4:36 Package: com.xingcloud.webinterface.plan
 */
public class TestUserGroupByQueryLogicalPlanNoSeg extends TestLogicalPlanBase {

  @Test
  public void testBuildPlan() throws Exception {
    String name = "groupby.prop.noseg.json";
    FormulaQueryDescriptor fqd = new GroupByFormulaQueryDescriptor(TEST_TABLE, TEST_REAL_BEGIN_DATE, TEST_REAL_END_DATE,
                                                                   TEST_EVENT, null, Filter.ALL, 1d, "register_time",
                                                                   GroupByType.USER_PROPERTIES);
    SegmentEvaluator.evaluate(fqd);
    LogicalPlan logicalPlan = fqd.toLogicalPlain();
    String planString = Plans.DEFAULT_DRILL_CONFIG.getMapper().writeValueAsString(logicalPlan);
    System.out.println(planString);
    write2File(name, planString);
    Submit submit = (Submit) SERVICE;
    submit.submit(fqd.getKey(), planString, Submit.SubmitQueryType.PLAN);
  }
}
