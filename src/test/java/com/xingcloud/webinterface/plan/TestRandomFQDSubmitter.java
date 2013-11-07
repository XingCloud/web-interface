package com.xingcloud.webinterface.plan;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xingcloud.basic.utils.DateUtils;
import com.xingcloud.qm.service.Submit;
import com.xingcloud.webinterface.enums.CommonQueryType;
import com.xingcloud.webinterface.enums.GroupByType;
import com.xingcloud.webinterface.enums.Interval;
import com.xingcloud.webinterface.exception.PlanException;
import com.xingcloud.webinterface.exception.SegmentException;
import com.xingcloud.webinterface.model.Filter;
import com.xingcloud.webinterface.model.formula.CommonFormulaQueryDescriptor;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import com.xingcloud.webinterface.model.formula.GroupByFormulaQueryDescriptor;
import com.xingcloud.webinterface.segment.SegmentEvaluator;
import com.xingcloud.webinterface.utils.WebInterfaceRandomUtils;
import org.apache.drill.common.logical.LogicalPlan;
import org.junit.Test;

import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * User: Z J Wu Date: 13-8-23 Time: 下午6:24 Package: com.xingcloud.webinterface.plan
 */
public class TestRandomFQDSubmitter extends TestLogicalPlanBase {

  @Test
  public void Test() throws Exception {
    String name = "random-plan";

    String[] segmentPool = new String[]{
      "{\"identifier\":[{\"op\":\"eq\",\"expr\":\"android.global.s77\",\"type\":\"CONST\"}]}",
      "{\"abflag\":[{\"op\":\"eq\",\"expr\":\"NewUIBarrack_NewNewbie_HeroHeadZoom_TroopAddConfig_B\",\"type\":\"CONST\"}]}",
      "{\"register_time\":[{\"op\":\"eq\",\"expr\":\"2013-07-01\",\"type\":\"CONST\"}]}"
    };
    Date targetDate = DateUtils.short2Date("2013-08-01");
    Random random = new Random();
    int times = 1;
    FormulaQueryDescriptor descriptor;
    boolean commonQuery;
    Date d1, d2;
    String s1, s2, seg;
    Submit submit = (Submit) SERVICE;
    Map<String, String> planStrings = new HashMap<String, String>(times);

    Interval[] intervalPool = new Interval[]{Interval.HOUR, Interval.PERIOD};

    String event;
    for (int i = 0; i < times; i++) {
      commonQuery = random.nextBoolean();
      if (random.nextBoolean()) {
        seg = segmentPool[random.nextInt(segmentPool.length)];
      } else {
        seg = null;
      }
      d1 = WebInterfaceRandomUtils.randomDate(targetDate, 20);
      d2 = DateUtils.dateAdd(d1, random.nextInt(10));
      s1 = DateUtils.date2Short(d1);
      s2 = DateUtils.date2Short(d2);
      event = random.nextBoolean() ? TEST_EVENT : TEST_EVENT_VISIT;
      if (commonQuery) {
        Interval interval = intervalPool[random.nextInt(intervalPool.length)];
        if (Interval.PERIOD.equals(interval)) {
          descriptor = new CommonFormulaQueryDescriptor(TEST_TABLE, s1, s2, event, seg, seg, Filter.ALL, interval,
                                                        CommonQueryType.NORMAL);
        } else {
          descriptor = new CommonFormulaQueryDescriptor(TEST_TABLE, s1, s1, event, seg, seg, Filter.ALL, interval,
                                                        CommonQueryType.NORMAL);
        }
      } else {
        descriptor = new GroupByFormulaQueryDescriptor(TEST_TABLE, s1, s2, event, seg, seg, Filter.ALL, "ref",
                                                       GroupByType.USER_PROPERTIES);
      }
      SegmentEvaluator.evaluate(descriptor);
      LogicalPlan logicalPlan = descriptor.toLogicalPlain();
      String planString = Plans.DEFAULT_DRILL_CONFIG.getMapper().writeValueAsString(logicalPlan);
      write2File(name + "." + i + ".json", planString);
      planStrings.put(descriptor.getKey(), planString);
    }

    for (Map.Entry<String, String> entry : planStrings.entrySet()) {
      System.out.println(entry.getKey());
//      submit.submit(entry.getKey(), entry.getValue(), Submit.SubmitQueryType.PLAN);
    }
  }

  @Test
  public void testGenerateLogicalPlan() throws ParseException, PlanException, SegmentException,
    JsonProcessingException {
    String pID = "age";
    String[] segmentPool = new String[]{
      "{\"identifier\":[{\"op\":\"eq\",\"expr\":\"android.global.s77\",\"type\":\"CONST\"}]}",
      "{\"abflag\":[{\"op\":\"eq\",\"expr\":\"NewUIBarrack_NewNewbie_HeroHeadZoom_TroopAddConfig_B\",\"type\":\"CONST\"}]}",
      "{\"register_time\":[{\"op\":\"eq\",\"expr\":\"2013-07-01\",\"type\":\"CONST\"}]}"
    };
    String[] eventsPool = new String[]{"visit.*", "pay.*", "*.*", "response.*.*.*.show.*", "response.*.*.*.pend.*",
                                       "response.*.*.*.responsetime.*"
    };
    Interval[] intervalPool = new Interval[]{Interval.MIN5, Interval.HOUR, Interval.PERIOD};

    String beginDate = "2013-09-01";
    String endDate = "2013-09-10";
    FormulaQueryDescriptor desc = new CommonFormulaQueryDescriptor(pID, beginDate, endDate, eventsPool[0],
                                                                   segmentPool[2], segmentPool[2], Filter.ALL,
                                                                   intervalPool[1], CommonQueryType.NORMAL);
    SegmentEvaluator.evaluate(desc);
    LogicalPlan logicalPlan = desc.toLogicalPlain();
    String planString = Plans.DEFAULT_DRILL_CONFIG.getMapper().writeValueAsString(logicalPlan);
    System.out.println(planString);

  }

}
