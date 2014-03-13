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
public class TestCommonQueryLogicalPlanNoSeg extends TestLogicalPlanBase {

  @Test
  public void testBuildPlan() throws Exception {
    String name = "common.day.noseg.json";
    FormulaQueryDescriptor fqd = new CommonFormulaQueryDescriptor("sof-apptools", "2014-02-20", "2014-02-20",
                                                                  "stat.connect.normal.finialsuccess.wifi.recordphone",
                                                                  null, Filter.ALL, "2014-02-20", "2014-02-20",
                                                                  Interval.DAY, CommonQueryType.NORMAL);
    SegmentEvaluator.evaluate(fqd);
    LogicalPlan logicalPlan = fqd.toLogicalPlain();
    String planString = Plans.DEFAULT_DRILL_CONFIG.getMapper().writeValueAsString(logicalPlan);
    System.out.println(planString);
    write2File(name, planString);
    Submit submit = (Submit) SERVICE;
    for (int i = 0; i < 1; i++) {
      submit.submit(fqd.getKey(), planString, Submit.SubmitQueryType.PLAN);
    }
  }
}
