package com.xingcloud.webinterface.event;

import com.xingcloud.webinterface.enums.CommonQueryType;
import com.xingcloud.webinterface.enums.Interval;
import com.xingcloud.webinterface.enums.Operator;
import com.xingcloud.webinterface.exception.SegmentException;
import com.xingcloud.webinterface.model.Filter;
import com.xingcloud.webinterface.model.formula.CommonFormulaQueryDescriptor;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import com.xingcloud.webinterface.segment.SegmentEvaluator;
import org.junit.Test;

import java.util.Map;

/**
 * User: Z J Wu Date: 13-9-25 Time: 上午11:41 Package: com.xingcloud.webinterface.event
 */
public class TestSegment {

  @Test
  public void test() throws SegmentException {
    String segment = "{\"register_time\":[{\"op\":\"gte\",\"expr\":\"$date_add(0)\",\"type\":\"VAR\"},{\"op\":\"lte\",\"expr\":\"$date_add(0)\",\"type\":\"VAR\"}]}";
    FormulaQueryDescriptor fqd = new CommonFormulaQueryDescriptor("age", "2013-09-01", "2013-09-01", "visit.*", segment,
                                                                  Filter.ALL, 1d, Interval.HOUR,
                                                                  CommonQueryType.NORMAL);

    SegmentEvaluator.evaluate(fqd);
    System.out.println(fqd);
    Map<String, Map<Operator, Object>> segmentMap = fqd.getSegmentMap();
    for (Map.Entry<String, Map<Operator, Object>> entry1 : segmentMap.entrySet()) {
      System.out.println(entry1.getKey());
      for (Map.Entry<Operator, Object> entry2 : entry1.getValue().entrySet()) {
        System.out.println("\t" + entry2.getKey() + " - " + entry2.getValue());
      }
    }

  }
}
