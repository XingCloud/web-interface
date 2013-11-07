package com.xingcloud.webinterface.sql;

import static com.xingcloud.webinterface.plan.Plans.buildUidEQJoinCondition;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.TOTAL_USER;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xingcloud.events.XEvent;
import com.xingcloud.events.XEventException;
import com.xingcloud.webinterface.enums.CommonQueryType;
import com.xingcloud.webinterface.enums.Interval;
import com.xingcloud.webinterface.exception.SegmentException;
import com.xingcloud.webinterface.model.Filter;
import com.xingcloud.webinterface.model.formula.CommonFormulaQueryDescriptor;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import com.xingcloud.webinterface.plan.Plans;
import com.xingcloud.webinterface.sql.visitor.SegmentSelectVisitor;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.common.logical.data.Join;
import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.logical.data.Store;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * User: Z J Wu Date: 13-10-28 Time: 下午5:02 Package: com.xingcloud.webinterface.sql
 */
public class SqlParser {
  private static SqlParser instance;

  public synchronized static SqlParser getInstance() {
    if (instance == null) {
      instance = new SqlParser();
    }
    return instance;
  }

  private SqlParser() {
  }

  private CCJSqlParserManager pm = new CCJSqlParserManager();

  private boolean isSelect(Statement statement) {
    return statement instanceof Select;
  }

  public LogicalOperator toLogicalOperator(FormulaQueryDescriptor fqd, List<LogicalOperator> operators) throws
    SegmentException {
    try {
      return _toLogicalOperator(fqd, operators);
    } catch (Exception e) {
      throw new SegmentException(e);
    }
  }

  private LogicalOperator _toLogicalOperator(FormulaQueryDescriptor fqd, List<LogicalOperator> operators) throws
    SegmentException, JSQLParserException {
    String sqlSegments = fqd.getSqlSegments();
    if (StringUtils.isBlank(sqlSegments) || TOTAL_USER.equals(sqlSegments)) {
      return null;
    }
    String[] segments = sqlSegments.split(";");
    System.out.println(Arrays.toString(segments));
    if (ArrayUtils.isEmpty(segments)) {
      return null;
    }
    Statement statement;
    SegmentSelectVisitor ssv;
    SelectBody selectBody;
    LogicalOperator lo1 = null, lo2;
    JoinCondition[] joinConditions = new JoinCondition[]{buildUidEQJoinCondition()};

    SegmentDescriptor sd;
    for (int i = 0; i < segments.length; i++) {
      statement = pm.parse(new StringReader(segments[i]));
      if (!isSelect(statement)) {
        throw new SegmentException("Unsupported sql operation - " + statement.getClass());
      }
      selectBody = ((Select) statement).getSelectBody();
      ssv = new SegmentSelectVisitor(fqd, operators);
      selectBody.accept(ssv);
      if (ssv.isWrong()) {
        throw new SegmentException(ssv.getException());

      }
      lo2 = ssv.getLogicalOperator();
      operators.add(lo2);
      sd = ssv.getSegmentDescriptor();
      System.out.println(sd);
      if (lo1 == null) {
        lo1 = lo2;
      } else {
        lo1 = new Join(lo1, lo2, joinConditions, Join.JoinType.INNER);
        operators.add(lo1);
      }
    }
    return lo1;
  }

  @Deprecated
  private String transformEvent(String projectId, String sql) throws XEventException {
    String eventKeywords = "event='";
    int l = eventKeywords.length();
    String eventEndKeywords = "'";
    int i, j, pos = 0;
    String eventString;
    XEvent xEvent;
    String[] eventArray;
    StringBuilder newSqlPartSB;
    StringBuilder newSqlSB = new StringBuilder();
    boolean atLeastOne = false;
    for (; ; ) {
      i = sql.indexOf(eventKeywords, pos);
      if (i < 0) {
        if (atLeastOne) {
          newSqlSB.append(sql.substring(pos + 1));
        } else {
          newSqlSB.append(sql.substring(pos));
        }
        break;
      }
      atLeastOne = true;
      newSqlSB.append(sql.substring(pos, i));
      j = sql.indexOf(eventEndKeywords, i + l);
      pos = j;
      eventString = StringUtils.trim(sql.substring(i + l, j));
      xEvent = XEvent.buildXEvent(projectId, eventString);
      if (xEvent == null) {
        throw new XEventException("No such event - " + xEvent);
      }
      eventArray = xEvent.getEventArray();
      newSqlPartSB = new StringBuilder();
      newSqlPartSB.append("event0");
      newSqlPartSB.append("='");
      newSqlPartSB.append(eventArray[0]);
      newSqlPartSB.append("'");

      for (int k = 1; k < eventArray.length; k++) {
        if (eventArray[k] != null) {
          newSqlPartSB.append(" and event");
          newSqlPartSB.append(k);
          newSqlPartSB.append("='");
          newSqlPartSB.append(eventArray[k]);
          newSqlPartSB.append("'");
        }
      }
      newSqlSB.append(newSqlPartSB.toString());
    }
    return newSqlSB.toString();
  }

  public static void main(String[] args) throws JSQLParserException, JsonProcessingException, XEventException,
    SegmentException {
    SqlParser paser = SqlParser.getInstance();
    String sql = "select uid from ((select uid from deu_age where event='visit.*.b' and date>=date_add('s',2) and date<=date_add('e',0)) as deu1 anti join (select uid from deu_age where event='visit.*' and date>=date_add('s',0) and date<= date_add('e',-2)) as deu2 on deu1.uid=deu2.uid)";
//    sql = "select uid from deu_age where date>=date_add('s',3) and event='visit.*.b' and date<=date_add('e',0)";
//    sql = "select uid from deu_age where date ='2013-11-05' and event='visit.*.b'";
//    sql = "select uid from user where register_time>=date_add('s',0) and register_time<=date_add('e',0) and grade > '100' and identifier in (nkj,reogn,cxkvzn,zxcnv)";
//    sql = "select uid from user where register_time>=date_add('s',0) and register_time<=date_add('e',0)";
//    sql = "select uid from user where register_time=date_add('s',0) ";
    sql = "select uid from ((select uid from deu_age where event='visit.*.b' and date>=date_add('s',2) and date<=date_add('e',0)) as deu1 anti join (select uid from deu_age where event='visit.*' and date>=date_add('s',0) and date<= date_add('e',-2)) as deu2 on deu1.uid=deu2.uid);select uid from user where grade in ('1',2,3);select uid from deu_age where date ='2013-11-05' and event='pay.*'";
//    sql = "select uid from deu_age where date ='2013-11-05' and event='visit.*.b';select uid from user where identifier=a and grade in ('1',2,3);select uid from user where register_time>=date_add('s',0) and register_time<=date_add('e',0)";
//    sql = "select uid from ((select uid from deu_age where date='2013-11-06' and event='visit.*') as deu1 anti join (select uid from deu_age where date='2013-11-07' and event='visit.*') as deu2 on deu1.uid=deu2.uid)";
    FormulaQueryDescriptor descriptor = new CommonFormulaQueryDescriptor("age", "2013-10-01", "2013-10-01", "visit.*",
                                                                         sql, sql, Filter.ALL, Interval.PERIOD,
                                                                         CommonQueryType.NORMAL);
    List<LogicalOperator> operators = new ArrayList<LogicalOperator>();
    LogicalOperator lo = paser.toLogicalOperator(descriptor, operators);
    Store store = new Store("DEFAULT-STORE", null, null);
    store.setInput(lo);
    operators.add(store);

    LogicalPlan lp = new LogicalPlan(Plans.buildPlanProperties("a"), new HashMap<String, StorageEngineConfig>(),
                                     operators);
//    System.out.println(Plans.DEFAULT_DRILL_CONFIG.getMapper().writeValueAsString(lp));

  }
}
