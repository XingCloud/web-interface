package com.xingcloud.webinterface.plan;

import com.xingcloud.webinterface.enums.CommonQueryType;
import com.xingcloud.webinterface.enums.Interval;
import com.xingcloud.webinterface.model.Filter;
import com.xingcloud.webinterface.model.formula.CommonFormulaQueryDescriptor;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import com.xingcloud.webinterface.sql.SqlSegmentParser;
import org.apache.drill.common.logical.LogicalPlan;
import org.junit.Test;

/**
 * User: Z J Wu Date: 13-8-6 Time: 下午4:36 Package: com.xingcloud.webinterface.plan
 */
public class TestCommonQueryLogicalPlanSeg extends TestLogicalPlanBase {

  @Test
  public void testBuildPlan() throws Exception {
    String name = "common.day.withseg.json";
    String sqlSegment;
    // user segment
    sqlSegment = "select uid from user where grade > '100'";
    sqlSegment = "select uid from user where grade > 100";
    sqlSegment = "select uid from user where grade in (2,4,'1','0',3)";
//    sqlSegment = "select uid from user where identifier in (nkj,reogn,cxkvzn,zxcnv)";
//    sqlSegment = "select uid from user where identifier in ('nkj',reogn,'cxkvzn',zxcnv)";
    sqlSegment = "select uid from user where register_time >= '2013-11-01' and register_time <= '2013-11-02'";
    sqlSegment = "select uid from user where register_time = '2013-11-01'";
    sqlSegment = "select uid from user where register_time > '2013-11-01'";
    sqlSegment = "select uid from user where first_pay_time>=date_add('s',0) and first_pay_time<=date_add('e',0)";
    sqlSegment = "select uid from user where first_pay_time>=date_add('s',0)";
    sqlSegment = "select uid from user where first_pay_time>=date_add('s',0);select uid from user where first_pay_time<=date_add('e',0)";
    sqlSegment = "select uid from user where register_time>=date_add('s',0) and register_time<=date_add('e',0) and grade > '100' and identifier in (nkj,reogn,cxkvzn,zxcnv)";

    // deu segment
//    sqlSegment = "select uid from user where grade > 100;select uid from deu where date = '2013-11-01' and event='pay.*';select uid from deu where event='pay.*' and date = '2013-11-01'";
    sqlSegment = "select uid from deu where event='pay.*' and date >= '2013-10-01' and date < '2013-10-03'";
//    sqlSegment = "select uid from deu where event='pay.*' and date = '2013-10-01';select uid from deu where event='buy.*' and date = '2013-10-03'";
//    sqlSegment = "select uid from ((select uid from deu_age where event='buy.banana.*' and date>=date_add('s',2) and date<=date_add('e',0)) as deu1 anti join (select uid from deu_age where event='buy.apple.*' and date>=date_add('s',0) and date<= date_add('e',-2)) as deu2 on deu1.uid=deu2.uid)";

    // Complex
//    sqlSegment = "select uid from user where first_pay_time>=date_add('s',0);" +
//      "select uid from user where register_time >= '2013-11-01' and register_time <= '2013-11-02';" +
//      "select uid from user where identifier in ('nkj',reogn,'cxkvzn',zxcnv);" +
//      "select uid from user where grade > 100;" +
//      "select uid from deu where event='pay.*' and date = '2013-11-01';" +
//      "select uid from ((select uid from deu_age where event='buy.banana.*' and date>=date_add('s',1) and date<=date_add('e',0)) as deu1 anti join (select uid from deu_age where event='buy.apple.*' and date>=date_add('s',0) and date<= date_add('e',-1)) as deu2 on deu1.uid=deu2.uid);";

//    sqlSegment="select uid from ((select uid from event where event='visit.*' and date>=date_add('s',14) and date<date_add('e',7)) as deu1 anti join (select uid from event where event='visit.*' and date>=date_add('s',7) and date<date_add('e',0)) as deu2 on deu1.uid=deu2.uid); select uid from user where first_pay_time>'2000-01-01'";
    FormulaQueryDescriptor fqd = new CommonFormulaQueryDescriptor("ram", TEST_REAL_BEGIN_DATE, TEST_REAL_END_DATE,
                                                                  TEST_EVENT_VISIT, sqlSegment, Filter.ALL,
                                                                  "2013-03-10", "2013-03-10", Interval.PERIOD,
                                                                  CommonQueryType.NORMAL);
    SqlSegmentParser.getInstance().evaluate(fqd);

    LogicalPlan logicalPlan = fqd.toLogicalPlain();
    String planString = Plans.DEFAULT_DRILL_CONFIG.getMapper().writeValueAsString(logicalPlan);
    System.out.println(planString);
    write2File(name, planString);

    logicalPlan = Plans.DEFAULT_DRILL_CONFIG.getMapper().readValue(planString, LogicalPlan.class);
    System.out.println(logicalPlan);
//    Submit submit = (Submit) SERVICE;
//    submit.submit(fqd.getKey(), planString, Submit.SubmitQueryType.PLAN);
  }
}
