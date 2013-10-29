package com.xingcloud.webinterface.sql;

import com.xingcloud.webinterface.sql.visitor.SegmentSelectVisitor;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import org.apache.drill.common.expression.LogicalExpression;

import java.io.StringReader;

/**
 * User: Z J Wu Date: 13-10-28 Time: 下午5:02 Package: com.xingcloud.webinterface.sql
 */
public class SqlPaser {
  private static SqlPaser instance;

  public synchronized static SqlPaser getInstance() {
    if (instance == null) {
      instance = new SqlPaser();
    }
    return instance;
  }

  private SqlPaser() {
  }

  private CCJSqlParserManager pm = new CCJSqlParserManager();

  private boolean isSelect(Statement statement) {
    return statement instanceof Select;
  }

  public LogicalExpression toLE(String sql) throws JSQLParserException {
    Statement statement = pm.parse(new StringReader(sql));
    if (!isSelect(statement)) {
      throw new JSQLParserException("Unsupported sql operation - " + statement.getClass());
    }
    SegmentSelectVisitor ssv = new SegmentSelectVisitor();
    SelectBody selectBody = ((Select) statement).getSelectBody();
    selectBody.accept(ssv);
    return ssv.getSegmentLogicalExpression();
  }

  public static void main(String[] args) throws JSQLParserException {
    SqlPaser paser = SqlPaser.getInstance();
    String sql = "select * from (a inner join b on a.uid = b.uid) where event=visit and date>date_add(0) and date<date_add(1)";
    paser.toLE(sql);
    sql = "select * from a where event=visit and date>date_add(0) and date<date_add(1)";
    paser.toLE(sql);
  }
}
