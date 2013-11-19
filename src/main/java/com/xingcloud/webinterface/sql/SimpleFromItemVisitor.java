package com.xingcloud.webinterface.sql;

import static com.xingcloud.webinterface.enums.SegmentTableType.E;
import static com.xingcloud.webinterface.enums.SegmentTableType.U;

import com.xingcloud.webinterface.enums.SegmentTableType;
import com.xingcloud.webinterface.sql.desc.SqlDescriptor;
import com.xingcloud.webinterface.sql.desc.TD;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import org.apache.commons.lang3.StringUtils;

/**
 * User: Z J Wu Date: 13-11-19 Time: 上午10:28 Package: com.xingcloud.webinterface.sql
 */
public class SimpleFromItemVisitor implements FromItemVisitor {

  private SegmentTableType segmentTableType;

  private String alias;

  private boolean singleTable;

  private SqlDescriptor sqlDescriptor;

  public SqlDescriptor getSqlDescriptor() {
    return sqlDescriptor;
  }

  public SegmentTableType getSegmentTableType() {
    return segmentTableType;
  }

  public String getAlias() {
    return alias;
  }

  public boolean isSingleTable() {
    return singleTable;
  }

  @Override
  public void visit(Table table) {
    this.singleTable = true;
    String alias = table.getAlias();
    String tName = table.getName();
    if (StringUtils.isBlank(alias)) {
      this.alias = tName;
    } else {
      this.alias = alias;
    }
    this.segmentTableType = (tName.contains("event") || tName.contains("deu")) ? E : U;
    this.sqlDescriptor = new TD();
  }

  @Override
  public void visit(SubSelect subSelect) {
    SimpleSelectVisitor ssv = new SimpleSelectVisitor();
    subSelect.getSelectBody().accept(ssv);
//    System.out.println("sub->"+subSelect.getAlias());
    this.alias = subSelect.getAlias();
  }

  @Override
  public void visit(SubJoin join) {
    FromItem leftFromItem = join.getLeft();
    SimpleFromItemVisitor sfv = new SimpleFromItemVisitor();
    leftFromItem.accept(sfv);
    sfv.getSqlDescriptor();

    Join thisJoin = join.getJoin();
    FromItem rightFromItem = thisJoin.getRightItem();
    SimpleFromItemVisitor rightFromItemVisitor = new SimpleFromItemVisitor();
    rightFromItem.accept(rightFromItemVisitor);
    System.out.println("Left:" + sfv.getAlias() + "|" + sfv.isSingleTable() + ", right:" + rightFromItemVisitor
      .getAlias() + "|" + rightFromItemVisitor.isSingleTable());


  }
}
