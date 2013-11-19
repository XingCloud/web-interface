package com.xingcloud.webinterface.sql;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;

import java.io.StringReader;

/**
 * User: Z J Wu Date: 13-11-19 Time: 上午10:01 Package: com.xingcloud.webinterface.sql
 */
public class TestSql {
  public static void main(String[] args) throws JSQLParserException {
    CCJSqlParserManager pm = new CCJSqlParserManager();
    String sql = "select uid from (event as t3 inner join (select uid from ( event as t1 anti join event as t2 on t1.uid=t2.uid) where t1.event='a' and t2.event='b') as t4 on t3.uid=t4.uid) where t3.event='c'";
    sql="select uid from ( event as t1 anti join event as t2 on t1.uid=t2.uid) where t1.event='a' and t2.event='b'";
    Statement statement = pm.parse(new StringReader(sql));
    Select select = (Select) statement;
    SelectBody selectBody = select.getSelectBody();
    SimpleSelectVisitor psv = new SimpleSelectVisitor();
    selectBody.accept(psv);

  }
}
