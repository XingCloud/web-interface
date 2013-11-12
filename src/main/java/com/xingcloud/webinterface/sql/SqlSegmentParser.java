package com.xingcloud.webinterface.sql;

import static com.xingcloud.webinterface.plan.Plans.buildUidEQJoinCondition;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xingcloud.events.XEventException;
import com.xingcloud.webinterface.enums.CommonQueryType;
import com.xingcloud.webinterface.enums.Interval;
import com.xingcloud.webinterface.exception.SegmentException;
import com.xingcloud.webinterface.model.Filter;
import com.xingcloud.webinterface.model.formula.CommonFormulaQueryDescriptor;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import com.xingcloud.webinterface.segment.XSegment;
import com.xingcloud.webinterface.sql.desc.JoinDescriptor;
import com.xingcloud.webinterface.sql.desc.SegmentDescriptor;
import com.xingcloud.webinterface.sql.desc.TableDescriptor;
import com.xingcloud.webinterface.sql.visitor.SegmentSelectVisitor;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.drill.common.logical.data.Join;
import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.log4j.Logger;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * User: Z J Wu Date: 13-10-28 Time: 下午5:02 Package: com.xingcloud.webinterface.sql
 */
public class SqlSegmentParser {
  private static final Logger LOGGER = Logger.getLogger(SqlSegmentParser.class);
  private static SqlSegmentParser instance;

  public synchronized static SqlSegmentParser getInstance() {
    if (instance == null) {
      instance = new SqlSegmentParser();
    }
    return instance;
  }

  private SqlSegmentParser() {
  }

  private CCJSqlParserManager pm = new CCJSqlParserManager();

  private boolean isSelect(Statement statement) {
    return statement instanceof Select;
  }

  public void evaluate(List<FormulaQueryDescriptor> descriptors) throws SegmentException {
    if (CollectionUtils.isEmpty(descriptors)) {
      return;
    }
    for (FormulaQueryDescriptor descriptor : descriptors) {
      evaluate(descriptor);
    }
  }

  public void evaluate(FormulaQueryDescriptor descriptor) throws SegmentException {
    if (!descriptor.hasSegment()) {
      descriptor.setSegment(null);
      return;
    }
    List<LogicalOperator> logicalOperators = new ArrayList<LogicalOperator>(3);
    XSegment segment;
    try {
      segment = parseSegment(descriptor, logicalOperators);
    } catch (SegmentException e) {
      throw e;
    } catch (JSQLParserException e) {
      throw new SegmentException(e);
    }
    descriptor.setSegment(segment);
  }

  private XSegment parseSegment(FormulaQueryDescriptor descriptor, List<LogicalOperator> logicalOperators) throws
    SegmentException, JSQLParserException {
    String[] segments = descriptor.getSqlSegments().split(";");
    if (ArrayUtils.isEmpty(segments)) {
      return null;
    }
    for (String sgmt : segments) {
      LOGGER.info("[SEGMENT] - Segment part - " + sgmt);
    }
    Set<String> segmentSet = new HashSet<String>(segments.length);
    for (String sql : segments) {
      segmentSet.add(sql);
    }
    Statement statement;
    SegmentSelectVisitor ssv;
    SelectBody selectBody;
    LogicalOperator lo1 = null, lo2;
    JoinCondition[] joinConditions = new JoinCondition[]{buildUidEQJoinCondition()};

    TableDescriptor td;
    JoinDescriptor jd;
    SegmentDescriptor sd = new SegmentDescriptor();
    for (String sql : segmentSet) {
      statement = pm.parse(new StringReader(sql));
      if (!isSelect(statement)) {
        throw new SegmentException("Unsupported sql operation - " + statement.getClass());
      }
      selectBody = ((Select) statement).getSelectBody();
      ssv = new SegmentSelectVisitor(descriptor, logicalOperators);
      selectBody.accept(ssv);
      if (ssv.isWrong()) {
        throw new SegmentException(ssv.getException());
      }
      lo2 = ssv.getLogicalOperator();
      logicalOperators.add(lo2);
      jd = ssv.getJoinDescriptor();
      if (jd == null) {
        td = ssv.getTableDescriptor();
        if (td == null) {
          throw new SegmentException("JoinDescriptor and TableDescriptor are all null.");
        } else {
          sd.addDescriptor(td);
        }
      } else {
        sd.addDescriptor(jd);
      }
      if (lo1 == null) {
        lo1 = lo2;
      } else {
        lo1 = new Join(lo1, lo2, joinConditions, Join.JoinType.INNER);
        logicalOperators.add(lo1);
      }
    }
    return new XSegment(sd.toSegmentKey(), lo1, logicalOperators, sd.getFunctionalPropertiesMap(), sd);
  }

  public static void main(String[] args) throws JSQLParserException, JsonProcessingException, XEventException,
    SegmentException {
    SqlSegmentParser paser = SqlSegmentParser.getInstance();
    String sql = "select uid from ((select uid from deu_age where event='visit.*.b' and date>=date_add('s',2) and date<=date_add('e',0)) as deu1 anti join (select uid from deu_age where event='visit.*' and date>=date_add('s',0) and date<= date_add('e',-2)) as deu2 on deu1.uid=deu2.uid)";
    sql = "select uid from event where date='2013-11-02' and event='pay.*';" +
      "select uid from event where date='2013-11-01' and event='visit.*';" +
      "select uid from user where grade in (2,4,'1','0',3);" +
      "select uid from event where date='2013-11-03' and event='consume.*';" +
      "select uid from user where register_time>=date_add('s',0) and register_time<=date_add('e',0) and pay_amount > '100';" +
      "select uid from ((select uid from deu_age where event='buy.banana.*' and date>=date_add('s',2) and date<=date_add('e',0)) as deu1 anti join (select uid from deu_age where event='buy.apple.*' and date>=date_add('s',0) and date<= date_add('e',-2)) as deu2 on deu1.uid=deu2.uid)";

//    sql = "select uid from deu_age where date>=date_add('s',3) and event='visit.*.b' and date<=date_add('e',0)";
//    sql = "select uid from deu_age where date ='2013-11-05' and event='visit.*.b'";
    sql = "select uid from user where first_pay_time>=date_add('s',0) and first_pay_time<=date_add('e',0);select uid from user where register_time>=date_add('s',0) and register_time<=date_add('e',0) and grade > '100' and identifier in (nkj,reogn,cxkvzn,zxcnv)";
//    sql = "select uid from user where register_time>=date_add('s',0) and register_time<=date_add('e',0)";
//    sql = "select uid from deu_age where date ='2013-11-05' and event='visit.*.b';select uid from user where register_time=date_add('s',0);select uid from user where grade in ('1',2,3)";
//    sql = "select uid from user where grade in (2,4,'1','0',3)";
//    sql = "select uid from deu_age where date ='2013-11-05' and event='pay.*';select uid from ((select uid from deu_age where event='visit.*.b' and date>=date_add('s',2) and date<=date_add('e',0)) as deu1 anti join (select uid from deu_age where event='visit.*' and date>=date_add('s',0) and date<= date_add('e',-2)) as deu2 on deu1.uid=deu2.uid);select uid from user where grade in ('1',2,3)";
//    sql = "select uid from deu_age where date ='2013-11-05' and event='visit.*.b';select uid from user where identifier=a and grade in ('1',2,3);select uid from user where register_time>=date_add('s',0) and register_time<=date_add('e',0)";
//    sql = "select uid from ((select uid from deu_age where date='2013-11-06' and event='visit.*') as deu1 anti join (select uid from deu_age where date='2013-11-07' and event='visit.*') as deu2 on deu1.uid=deu2.uid)";

    FormulaQueryDescriptor descriptor = new CommonFormulaQueryDescriptor("age", "2013-11-08", "2013-11-08", "visit.*",
                                                                         sql, Filter.ALL, Interval.PERIOD,
                                                                         CommonQueryType.NORMAL);
    paser.evaluate(descriptor);
    System.out.println(descriptor);

//    System.out.println(Plans.DEFAULT_DRILL_CONFIG.getMapper().writeValueAsString(lp));

  }
}
