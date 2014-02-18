package com.xingcloud.webinterface.plan.common;

import com.xingcloud.qm.service.Submit;
import com.xingcloud.webinterface.enums.CommonQueryType;
import com.xingcloud.webinterface.enums.Interval;
import com.xingcloud.webinterface.model.Filter;
import com.xingcloud.webinterface.model.formula.CommonFormulaQueryDescriptor;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import com.xingcloud.webinterface.plan.Plans;
import com.xingcloud.webinterface.plan.TestLogicalPlanBase;
import com.xingcloud.webinterface.sql.SqlSegmentParser;
import org.apache.drill.common.logical.LogicalPlan;
import org.junit.Test;

/**
 * User: Z J Wu Date: 13-8-6 Time: 下午4:36 Package: com.xingcloud.webinterface.plan
 */
public class TestMin5QueryLogicalPlanSeg extends TestLogicalPlanBase {

  @Test
  public void testBuildPlan() throws Exception {
    String name = "common.hour.withseg.json";
    String sqlSegment;
    // user segment
    sqlSegment = "select uid from user where grade > '100'";
    sqlSegment = "select uid from user where grade > 100";
    sqlSegment = "select uid from user where grade in (2,4,'1','0',3)";
    sqlSegment = "select uid from user where identifier in (nkj,reogn,cxkvzn,zxcnv)";
    sqlSegment = "select uid from user where identifier in ('nkj',reogn,'cxkvzn',zxcnv)";
    sqlSegment = "select uid from user where register_time >= '2013-11-01' and register_time <= '2013-11-02'";
    sqlSegment = "select uid from user where register_time = '2013-11-01'";
    sqlSegment = "select uid from user where register_time > '2013-11-01'";
    sqlSegment = "select uid from user where first_pay_time>=date_add('s',0) and first_pay_time<=date_add('e',0)";
//    sqlSegment = "select uid from user where first_pay_time>=date_add('s',0)";
//    sqlSegment = "select uid from user where register_time>=date_add('s',0) and register_time<=date_add('e',0) and grade > '100' and identifier in (nkj,reogn,cxkvzn,zxcnv)";

    // deu segment
//    sqlSegment = "select uid from deu where event='pay.*' and date = '2013-11-01'";
//    sqlSegment = "select uid from deu where event='pay.*' and date >= '2013-10-01' and date <= '2013-10-07'";
//    sqlSegment = "select uid from deu where event='pay.*' and date = '2013-10-01';select uid from deu where event='buy.*' and date = '2013-10-02'";
    sqlSegment = "select uid from ((select uid from deu_age where event='buy.banana.*' and date>=date_add('s',2) and date<=date_add('e',0)) as deu1 anti join (select uid from deu_age where event='buy.apple.*' and date>=date_add('s',0) and date<= date_add('e',-2)) as deu2 on deu1.uid=deu2.uid)";

    FormulaQueryDescriptor fqd = new CommonFormulaQueryDescriptor(TEST_TABLE, TEST_REAL_BEGIN_DATE, TEST_REAL_END_DATE,
                                                                  TEST_EVENT, sqlSegment, Filter.ALL, "2013-03-10",
                                                                  "2013-03-12", Interval.HOUR, CommonQueryType.NORMAL);
    SqlSegmentParser.getInstance().evaluate(fqd);
    LogicalPlan logicalPlan = fqd.toLogicalPlain();
    String planString = Plans.DEFAULT_DRILL_CONFIG.getMapper().writeValueAsString(logicalPlan);
    System.out.println(planString);
//    write2File(name, planString);
//    Submit submit = (Submit) SERVICE;
//    submit.submit(fqd.getKey(), planString, Submit.SubmitQueryType.PLAN);
  }
}
