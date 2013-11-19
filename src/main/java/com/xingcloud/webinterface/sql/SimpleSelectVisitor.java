package com.xingcloud.webinterface.sql;

import com.xingcloud.webinterface.sql.desc.TD;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.Union;

/**
 * User: Z J Wu Date: 13-11-19 Time: 上午10:19 Package: com.xingcloud.webinterface.sql
 */
public class SimpleSelectVisitor implements SelectVisitor {
  @Override public void visit(PlainSelect plainSelect) {
    FromItem fromItem = plainSelect.getFromItem();
    SimpleFromItemVisitor sfv = new SimpleFromItemVisitor();
    fromItem.accept(sfv);
    if (sfv.isSingleTable()) {
      Expression whereClause = plainSelect.getWhere();
      TD td = new TD(whereClause);
    } else {

    }

  }

  @Override public void visit(Union union) {
  }
}
