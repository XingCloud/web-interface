package com.xingcloud.webinterface.plan;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xingcloud.webinterface.enums.CommonQueryType;
import com.xingcloud.webinterface.enums.Interval;
import com.xingcloud.webinterface.exception.PlanException;
import com.xingcloud.webinterface.exception.SegmentException;
import com.xingcloud.webinterface.model.Filter;
import com.xingcloud.webinterface.model.formula.CommonFormulaQueryDescriptor;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import com.xingcloud.webinterface.segment2.SegmentEvaluator;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

/**
 * User: Z J Wu Date: 13-8-21 Time: 上午11:44 Package: com.xingcloud.webinterface.plan
 */
public class TestMultiDescriptor {

  @Test
  public void testMultiDescriptor() throws SegmentException, PlanException, JsonProcessingException {
    String table = "age";
    String event = "visit.*";
    String segment1 = "{\"register_time\":[{\"op\":\"gte\",\"expr\":\"2013-08-14\",\"type\":\"CONST\"},{\"op\":\"lte\",\"expr\":\"2013-08-14\",\"type\":\"CONST\"}]}";
    String segment2 = "{\"register_time\":[{\"op\":\"gte\",\"expr\":\"2013-08-15\",\"type\":\"CONST\"},{\"op\":\"lte\",\"expr\":\"2013-08-15\",\"type\":\"CONST\"}]}";
    String segment3 = "{\"register_time\":[{\"op\":\"gte\",\"expr\":\"2013-08-16\",\"type\":\"CONST\"},{\"op\":\"lte\",\"expr\":\"2013-08-16\",\"type\":\"CONST\"}]}";
    String segment4 = "{\"register_time\":[{\"op\":\"gte\",\"expr\":\"2013-08-17\",\"type\":\"CONST\"},{\"op\":\"lte\",\"expr\":\"2013-08-17\",\"type\":\"CONST\"}]}";
    String segment5 = "{\"register_time\":[{\"op\":\"gte\",\"expr\":\"2013-08-14\",\"type\":\"CONST\"},{\"op\":\"lte\",\"expr\":\"2013-08-17\",\"type\":\"CONST\"}]}";
    FormulaQueryDescriptor[] descriptors = new FormulaQueryDescriptor[6];
    descriptors[0] = new CommonFormulaQueryDescriptor(table, "2013-08-14", "2013-08-14", event, segment1, Filter.ALL,
                                                      1d, "2013-03-10", "2013-03-12", Interval.PERIOD,
                                                      CommonQueryType.NORMAL);
    descriptors[1] = new CommonFormulaQueryDescriptor(table, "2013-08-15", "2013-08-15", event, segment2, Filter.ALL,
                                                      1d, "2013-08-15", "2013-08-15", Interval.PERIOD,
                                                      CommonQueryType.NORMAL);
    descriptors[2] = new CommonFormulaQueryDescriptor(table, "2013-08-16", "2013-08-16", event, segment3, Filter.ALL,
                                                      1d, "2013-08-16", "2013-08-16", Interval.PERIOD,
                                                      CommonQueryType.NORMAL);
    descriptors[3] = new CommonFormulaQueryDescriptor(table, "2013-08-17", "2013-08-17", event, segment4, Filter.ALL,
                                                      1d, "2013-08-17", "2013-08-17", Interval.PERIOD,
                                                      CommonQueryType.NORMAL);
    descriptors[4] = new CommonFormulaQueryDescriptor(table, "2013-08-14", "2013-08-17", event, segment5, Filter.ALL,
                                                      1d, "2013-08-14", "2013-08-17", Interval.PERIOD,
                                                      CommonQueryType.NORMAL);
    descriptors[5] = new CommonFormulaQueryDescriptor(table, "2013-08-14", "2013-08-17", event, null, Filter.ALL, 1d,
                                                      "2013-08-14", "2013-08-17", Interval.PERIOD,
                                                      CommonQueryType.NORMAL);

    File f;
    PrintWriter pw = null;
    String jsonString;
    int i = 0;
    for (FormulaQueryDescriptor fqd : descriptors) {
      ++i;
      SegmentEvaluator.evaluate(fqd);
      jsonString = fqd.toLogicalPlain().toJsonString(Plans.DEFAULT_DRILL_CONFIG);
      f = new File("D:/plans/lp." + i + ".json");
      try {
        pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(f)));
        pw.write(jsonString);
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        if (pw != null) {
          pw.close();
        }
      }
    }
  }
}
