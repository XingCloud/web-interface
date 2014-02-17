package com.xingcloud.webinterface.plan.common;

import com.xingcloud.qm.service.Submit;
import com.xingcloud.webinterface.enums.CommonQueryType;
import com.xingcloud.webinterface.enums.Interval;
import com.xingcloud.webinterface.model.Filter;
import com.xingcloud.webinterface.model.formula.CommonFormulaQueryDescriptor;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import com.xingcloud.webinterface.plan.Plans;
import com.xingcloud.webinterface.plan.TestLogicalPlanBase;
import com.xingcloud.webinterface.segment.SegmentEvaluator;
import org.apache.drill.common.logical.LogicalPlan;
import org.junit.Test;

/**
 * User: Z J Wu Date: 13-8-6 Time: 下午4:36 Package: com.xingcloud.webinterface.plan
 */
public class TestMin5QueryLogicalPlanSeg extends TestLogicalPlanBase {

  @Test
  public void testBuildPlan() throws Exception {
    String name = "common.hour.withseg.json";
    String segment = "{\"register_time\":[{\"op\":\"eq\",\"expr\":\"2013-09-25\",\"type\":\"CONST\"}]}";
    FormulaQueryDescriptor fqd = new CommonFormulaQueryDescriptor(TEST_TABLE, TEST_REAL_BEGIN_DATE, TEST_REAL_END_DATE,
                                                                  TEST_EVENT, segment, Filter.ALL, "2013-03-10",
                                                                  "2013-03-12", Interval.HOUR, CommonQueryType.NORMAL);
    SegmentEvaluator.evaluate(fqd);
    LogicalPlan logicalPlan = fqd.toLogicalPlain();
    String planString = Plans.DEFAULT_DRILL_CONFIG.getMapper().writeValueAsString(logicalPlan);
    System.out.println(planString);
    write2File(name, planString);
    Submit submit = (Submit) SERVICE;
    submit.submit(fqd.getKey(), planString, Submit.SubmitQueryType.PLAN);
  }
}
