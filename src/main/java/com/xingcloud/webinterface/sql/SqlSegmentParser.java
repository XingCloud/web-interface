package com.xingcloud.webinterface.sql;

import com.xingcloud.webinterface.exception.SegmentException;
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
import org.apache.log4j.Logger;

import java.io.StringReader;
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
    XSegment segment;
    try {
      segment = parseSegment(descriptor);
    } catch (SegmentException e) {
      throw e;
    } catch (JSQLParserException e) {
      throw new SegmentException(e);
    }
    descriptor.setSegment(segment);
  }

  private XSegment parseSegment(FormulaQueryDescriptor descriptor) throws SegmentException, JSQLParserException {
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
    TableDescriptor td;
    JoinDescriptor jd;
    SegmentDescriptor sd = new SegmentDescriptor(descriptor);
    for (String sql : segmentSet) {
      statement = pm.parse(new StringReader(sql));
      if (!isSelect(statement)) {
        throw new SegmentException("Unsupported sql operation - " + statement.getClass());
      }
      selectBody = ((Select) statement).getSelectBody();
      ssv = new SegmentSelectVisitor(descriptor);
      selectBody.accept(ssv);
      if (ssv.isWrong()) {
        throw new SegmentException(ssv.getException());
      }
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
    }
    String segmentIdentifier = sd.toSegment();
    return new XSegment(segmentIdentifier, sd.getRootSegmentLogicalOperator(), sd.getLogicalOperators(),
                        sd.getFunctionalPropertiesMap(), sd);
  }
}
