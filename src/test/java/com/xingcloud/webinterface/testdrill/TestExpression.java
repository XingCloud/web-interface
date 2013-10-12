package com.xingcloud.webinterface.testdrill;

import static com.xingcloud.webinterface.plan.Plans.buildPlanProperties;
import static com.xingcloud.webinterface.plan.Plans.getStore;
import static org.apache.drill.common.util.FieldReferenceBuilder.buildColumn;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xingcloud.webinterface.plan.Plans;
import com.xingcloud.webinterface.plan.ScanFilter;
import com.xingcloud.webinterface.plan.ScanSelection;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.common.logical.data.Store;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: Z J Wu Date: 13-10-10 Time: 下午4:55 Package: com.xingcloud.webinterface.testdrill
 */
public class TestExpression {

  @Test
  public void test() throws IOException {
    ObjectMapper mapper = Plans.DEFAULT_DRILL_CONFIG.getMapper();
//    LogicalExpression left = buildColumn("val");
//    LogicalExpression right = new ValueExpressions.QuotedString("\"null\"", ExpressionPosition.UNKNOWN);
//    LogicalExpression fc = Plans.DFR.createExpression("==", ExpressionPosition.UNKNOWN, left, right);
//
//    FieldReference fr = buildColumn("uid");
//    NamedExpression[] projections = new NamedExpression[]{new NamedExpression(fr, fr)};
//    ScanFilter sf = new ScanFilter(fc);
//    ScanSelection ss = new ScanSelection("abc", sf, projections);
//    ScanSelection[] sss = new ScanSelection[]{ss};
//
//    JSONOptions jp = mapper.readValue(mapper.writeValueAsString(sss), JSONOptions.class);
//
//    Scan scan = new Scan("hbase", jp, buildColumn("logical_scan"));
//
//    List<LogicalOperator> logicalOperators = new ArrayList<LogicalOperator>();
//    logicalOperators.add(scan);
//
//    Store store = getStore();
//    store.setInput(scan);
//    logicalOperators.add(store);
//
//    Map<String, StorageEngineConfig> storageEngineMap = new HashMap<String, StorageEngineConfig>();
//    LogicalPlan lp = new LogicalPlan(buildPlanProperties("abc"), storageEngineMap, logicalOperators);
//
//    String s = mapper.writeValueAsString(lp);
//    System.out.println(s);
//
//    lp = mapper.readValue(s, LogicalPlan.class);
//    System.out.println(lp);

    String str = "COMMON,ram,2013-10-01,2013-10-02,visit.*,{\"identifier\":\"\\\"null\\\"\"},VF-ALL-0-0,PERIOD";
    LogicalExpression qs = new ValueExpressions.QuotedString(str, ExpressionPosition.UNKNOWN);
    String str2 = mapper.writeValueAsString(qs);
    System.out.println(str2);
    System.out.println(mapper.readValue(str2, LogicalExpression.class));

  }
}
