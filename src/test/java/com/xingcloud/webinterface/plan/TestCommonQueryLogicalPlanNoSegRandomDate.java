package com.xingcloud.webinterface.plan;

import com.xingcloud.basic.utils.DateUtils;
import com.xingcloud.qm.service.Submit;
import com.xingcloud.webinterface.enums.CommonQueryType;
import com.xingcloud.webinterface.enums.Interval;
import com.xingcloud.webinterface.model.Filter;
import com.xingcloud.webinterface.model.formula.CommonFormulaQueryDescriptor;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import com.xingcloud.webinterface.sql.SqlSegmentParser;
import com.xingcloud.webinterface.utils.WebInterfaceRandomUtils;
import org.apache.drill.common.logical.LogicalPlan;
import org.junit.Test;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * User: Z J Wu Date: 13-8-6 Time: 下午4:36 Package: com.xingcloud.webinterface.plan
 */
public class TestCommonQueryLogicalPlanNoSegRandomDate extends TestLogicalPlanBase {

  @Test
  public void testBuildPlan() throws Exception {
    String name = "common.day.noseg.random";
    Date targetDate = DateUtils.short2Date("2013-07-01");
    Random r = new Random();
    Date d1, d2;
    String s1, s2;
    FormulaQueryDescriptor fqd;
    Submit submit = (Submit) SERVICE;
    int times = 1;
    Map<String, String> planStrings = new HashMap<String, String>(times);
    String event;
    for (int i = 0; i < times; i++) {
      d1 = WebInterfaceRandomUtils.randomDate(targetDate, 7);
      d2 = DateUtils.dateAdd(d1, r.nextInt(7));
      s1 = DateUtils.date2Short(d1);
      s2 = DateUtils.date2Short(d2);
      event = r.nextBoolean() ? TEST_EVENT : TEST_EVENT2;
      fqd = new CommonFormulaQueryDescriptor(TEST_TABLE, s1, s2, event, null, Filter.ALL,  "2013-03-10",
                                             "2013-03-12", Interval.PERIOD, CommonQueryType.NORMAL);

      SqlSegmentParser.getInstance().evaluate(fqd);
      LogicalPlan logicalPlan = fqd.toLogicalPlain();
      String planString = Plans.DEFAULT_DRILL_CONFIG.getMapper().writeValueAsString(logicalPlan);
      planStrings.put(fqd.getKey(), planString);
      write2File(name + "." + i + ".json", planString);
    }

//    for (Map.Entry<String, String> entry : planStrings.entrySet()) {
//      System.out.println(entry.getKey());
//      submit.submit(entry.getKey(), entry.getValue(), Submit.SubmitQueryType.PLAN);
//    }

  }
}
