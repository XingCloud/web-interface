package com.xingcloud.webinterface.plan;

import com.xingcloud.webinterface.enums.CommonQueryType;
import com.xingcloud.webinterface.enums.Interval;
import com.xingcloud.webinterface.model.Filter;
import com.xingcloud.webinterface.model.formula.CommonFormulaQueryDescriptor;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import com.xingcloud.webinterface.segment.SegmentEvaluator;
import org.apache.drill.common.logical.LogicalPlan;
import org.junit.Test;

/**
 * User: Z J Wu Date: 13-8-6 Time: 下午4:36 Package: com.xingcloud.webinterface.plan
 */
public class TestCommonQueryLogicalPlanSeg extends TestLogicalPlanBase {

  @Test
  public void testBuildPlan() throws Exception {
    String name = "common.day.withseg.json";
    String segment = "{" +
      "\"register_time\":[{\"op\":\"eq\",\"expr\":\"2013-08-31\",\"type\":\"CONST\"}]," +
      "\"first_pay_time\":[{\"op\":\"gte\",\"expr\":\"2013-07-31\",\"type\":\"CONST\"},{\"op\":\"lte\",\"expr\":\"2013-07-31\",\"type\":\"CONST\"}]," +
      "\"language\":[{\"op\":\"eq\",\"expr\":\"zh_cn\",\"type\":\"CONST\"}]" +
      "}";

    segment = "{\"register_time\":[{\"op\":\"eq\",\"expr\":\"2013-09-12\",\"type\":\"CONST\"}]}";
//    segment = "{\"identifier\":[{\"op\":\"eq\",\"expr\":\"\\\\\"null\\\\\"\",\"type\":\"CONST\"}]}";

    System.out.println(segment);

    FormulaQueryDescriptor fqd = new CommonFormulaQueryDescriptor("ram", TEST_REAL_BEGIN_DATE, TEST_REAL_END_DATE,
                                                                  TEST_EVENT_VISIT, segment, segment, Filter.ALL,
                                                                  "2013-03-10", "2013-03-12", Interval.PERIOD,
                                                                  CommonQueryType.NORMAL);
    SegmentEvaluator.evaluate(fqd);

    LogicalPlan logicalPlan = fqd.toLogicalPlain();
    String planString = Plans.DEFAULT_DRILL_CONFIG.getMapper().writeValueAsString(logicalPlan);
    System.out.println(planString);
    write2File(name, planString);

    logicalPlan=Plans.DEFAULT_DRILL_CONFIG.getMapper().readValue(planString,LogicalPlan.class);
    System.out.println(logicalPlan);
//    Submit submit = (Submit) SERVICE;
//    submit.submit(fqd.getKey(), planString, Submit.SubmitQueryType.PLAN);
  }
}
