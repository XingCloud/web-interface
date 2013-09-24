package com.xingcloud.webinterface.plan;

import static com.xingcloud.webinterface.enums.Operator.EQ;
import static com.xingcloud.webinterface.enums.Operator.GTE;
import static com.xingcloud.webinterface.enums.Operator.LTE;
import static org.apache.drill.common.expression.ExpressionPosition.UNKNOWN;
import static org.apache.drill.common.util.DrillConstants.SE_HBASE;
import static org.apache.drill.common.util.DrillConstants.SE_MYSQL;
import static org.apache.drill.common.util.FieldReferenceBuilder.buildColumn;
import static org.apache.drill.common.util.FieldReferenceBuilder.buildTable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xingcloud.events.XEvent;
import com.xingcloud.events.XEventException;
import com.xingcloud.memcache.MemCacheException;
import com.xingcloud.mysql.PropType;
import com.xingcloud.mysql.UserProp;
import com.xingcloud.webinterface.enums.Interval;
import com.xingcloud.webinterface.enums.Operator;
import com.xingcloud.webinterface.exception.PlanException;
import com.xingcloud.webinterface.model.BeginEndDatePair;
import com.xingcloud.webinterface.utils.DateSplitter;
import com.xingcloud.webinterface.utils.UserPropertiesInfoManager;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.PlanProperties;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.FunctionRegistry;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.logical.data.Join;
import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.common.logical.data.Store;
import org.apache.thrift.TException;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * User: Z J Wu Date: 13-8-5 Time: 下午2:58 Package: com.xingcloud.webinterface.utils
 */
public class Plans2 {
  public static final byte MIN_EVENT_BYTE = 0;
  public static final String KEY_WORD_GENERIC_EVENT_TABLE_NAME = "generic_project";
  public static final String KEY_WORD_PROJECTIONS = "projections";
  public static final String KEY_WORD_UID = "uid";
  public static final String KEY_WORD_EVENT = "event";
  public static final String KEY_WORD_USER = "user";
  public static final String KEY_WORD_VALUE = "value";
  public static final String KEY_WORD_VAL = "val";
  public static final String KEY_WORD_DIMENSION = "dimension";
  public static final String KEY_WORD_SGMT = "sgmt";
  public static final String KEY_WORD_TIMESTAMP = "timestamp";

  public static FunctionRegistry DFR = new FunctionRegistry(DrillConfig.create());

  public static DrillConfig DEFAULT_DRILL_CONFIG = DrillConfig.create();

  @Deprecated
  public static short propertyString2TinyInt(String projectId, String propName) throws Exception {
    UserProp up = UserPropertiesInfoManager.getInstance().getUserProp(projectId, propName);
    return (short) 1;
  }

  public static String toEventTableName(String projectId) {
    return "deu_" + projectId;
  }

  public static String toUserIndexTableName(String projectId) {
    return "property_" + projectId + "_index";
  }

  public static String toUserMysqlTableName(String projectId, String propName) {
    return projectId + "." + propName;
  }

  private static boolean usingDateFunc(String projectId, String propName) throws PlanException {
    UserProp up;
    try {
      up = UserPropertiesInfoManager.getInstance().getUserProp(projectId, propName);
    } catch (Exception e) {
      throw new PlanException(e);
    }
    PropType pt = up.getPropType();
    return PropType.sql_datetime.equals(pt);
  }

  private static boolean usingBeginSuffix(Operator op) {
    return GTE.equals(op);
  }

  private static Object object2String(boolean transformDate, boolean beginSuffix, Object o) {
    if (o instanceof Number || o instanceof Boolean) {
      return o;
    } else {
      if (transformDate) {
        String dateString = o.toString().replace("-", ""), longString;
        longString = beginSuffix ? dateString + "000000" : dateString + "235959";
        return Long.valueOf(longString);
      } else {
        return o.toString();
      }
    }
  }

  private static LogicalExpression toLE(Object o) {
    if (o instanceof String) {
      return new ValueExpressions.QuotedString(o.toString(), ExpressionPosition.UNKNOWN);
    } else if (o instanceof Number) {
      return ValueExpressions.getNumericExpression(o.toString(), ExpressionPosition.UNKNOWN);
    } else if (o instanceof Boolean) {
      return new ValueExpressions.BooleanExpression(o.toString(), ExpressionPosition.UNKNOWN);
    } else {
      return new FieldReference(o.toString(), ExpressionPosition.UNKNOWN);
    }
  }

  private static LogicalOperator getSingleMysqlSegmentExprScan(String projectId, String propertyName,
                                                               Map<Operator, Object> valueMap) throws PlanException {
    if (MapUtils.isEmpty(valueMap)) {
      throw new PlanException("Empty segment value");
    }
    String table = toUserMysqlTableName(projectId, propertyName);
    boolean transformDate = usingDateFunc(projectId, propertyName);
    Object valueObject;

    if (transformDate && valueMap.containsKey(EQ)) {
      valueObject = valueMap.get(EQ);
      valueMap.remove(EQ);
      valueMap.put(GTE, valueObject);
      valueMap.put(LTE, valueObject);
    }
    String sqlOperator;
    List<LogicalExpression> logicalExpressions = new ArrayList<LogicalExpression>(valueMap.size());
    String field = "val";
    Object fieldValue;
    FieldReference fieldReference = buildColumn(field);
    LogicalExpression fieldValueReference;
    Operator operator;
    boolean usingBeginSuffix;
    LogicalExpression le;

    for (Map.Entry<Operator, Object> entry : valueMap.entrySet()) {
      operator = entry.getKey();
      sqlOperator = operator.getMathOperator();
      usingBeginSuffix = usingBeginSuffix(operator);
      valueObject = entry.getValue();
      fieldValue = object2String(transformDate, usingBeginSuffix, valueObject);
      fieldValueReference = toLE(fieldValue);
      le = DFR.createExpression(sqlOperator, ExpressionPosition.UNKNOWN, fieldReference, fieldValueReference);
      logicalExpressions.add(le);
    }
    FieldReference fr = buildColumn(KEY_WORD_UID);
    NamedExpression[] projections = new NamedExpression[]{new NamedExpression(fr, fr)};

    ScanFilter sf = new ScanFilter(toBinaryExpression(logicalExpressions, "and"));
    ScanSelection ss = new ScanSelection(table, sf, projections);
    ScanSelection[] sss = new ScanSelection[]{ss};
    ObjectMapper mapper = DEFAULT_DRILL_CONFIG.getMapper();
    String str;
    try {
      str = mapper.writeValueAsString(sss);
      Scan scan = new Scan(SE_MYSQL, mapper.readValue(str, JSONOptions.class), buildTable(KEY_WORD_USER));
      scan.setMemo("Scan(Table=" + table + ", Prop=" + propertyName + ", Val=" + valueMap + ")");
      return scan;
    } catch (Exception e) {
      throw new PlanException(e);
    }
  }

  public static LogicalOperator getChainedMysqlSegmentScan(String projectId, List<LogicalOperator> operators,
                                                           Map<String, Map<Operator, Object>> segmentMap) throws
    PlanException {
    if (MapUtils.isEmpty(segmentMap)) {
      throw new PlanException("Empty segment");
    }
    Iterator<Map.Entry<String, Map<Operator, Object>>> it = segmentMap.entrySet().iterator();
    Map.Entry<String, Map<Operator, Object>> entry = it.next();
    String propertyName = entry.getKey();
    Map<Operator, Object> singleSegmentMap = entry.getValue();
    LogicalOperator lo1 = getSingleMysqlSegmentExprScan(projectId, propertyName, singleSegmentMap), lo2;
    operators.add(lo1);

    Join join;
    JoinCondition[] joinConditions;
    for (; ; ) {
      if (!it.hasNext()) {
        break;
      }
      entry = it.next();
      propertyName = entry.getKey();
      singleSegmentMap = entry.getValue();
      lo2 = getSingleMysqlSegmentExprScan(projectId, propertyName, singleSegmentMap);
      operators.add(lo2);

      joinConditions = new JoinCondition[1];
      joinConditions[0] = new JoinCondition("==", buildColumn(KEY_WORD_UID), buildColumn(KEY_WORD_UID));
      join = new Join(lo1, lo2, joinConditions, Join.JoinType.INNER);
      operators.add(join);
      lo1 = join;
    }
    return lo1;
  }

  public static LogicalOperator getUserScan(String projectId, String groupBy) throws PlanException {
    JSONOptions selection;
    String table = toUserMysqlTableName(projectId, groupBy);
    Map<String, Object> selectionMap = new HashMap<String, Object>(2);
    selectionMap.put("table", table);
    FieldReference uidFR = buildColumn(KEY_WORD_UID);
    NamedExpression[] projections = new NamedExpression[2];
    projections[0] = new NamedExpression(uidFR, uidFR);
    projections[1] = new NamedExpression(buildColumn(KEY_WORD_VAL), buildColumn(groupBy));

    selectionMap.put(KEY_WORD_PROJECTIONS, projections);
    String str;
    ObjectMapper mapper = DEFAULT_DRILL_CONFIG.getMapper();
    try {
      str = mapper.writeValueAsString(selectionMap);
      selection = mapper.readValue(str, JSONOptions.class);
    } catch (Exception e) {
      throw new PlanException(e);
    }
    Scan scan = new Scan(SE_MYSQL, selection, buildTable("user"));
    scan.setMemo("Table=" + table);
    return scan;
  }

  public static LogicalOperator getEventScan(String projectId, String event, String realBeginDate, String realEndDate,
                                             String... additionalProjections) throws PlanException {
    JSONOptions selection;
    XEvent parameterEvent;

    try {
      parameterEvent = XEvent.buildXEvent(projectId, event);
    } catch (Exception e) {
      throw new PlanException(e);
    }

    try {
      selection = getEventSelections(projectId, parameterEvent, realBeginDate, realEndDate, additionalProjections);
    } catch (Exception e) {
      throw new PlanException(e);
    }
    if (selection == null) {
      throw new PlanException("Selection of event table(" + projectId + ") is null.");
    }
    String eventTable = toEventTableName(projectId);
    Scan scan = new Scan(SE_HBASE, selection, buildTable(KEY_WORD_EVENT));
    scan.setMemo(eventTable + "," + realBeginDate + "," + realEndDate + "," + event);
    return scan;
  }

  private static LogicalExpression toBinaryExpression(Collection<LogicalExpression> logicalExpressions,
                                                      String binaryFunction) throws PlanException {
    if (StringUtils.isBlank(binaryFunction)) {
      throw new PlanException("Binary function cannot be empty while creating binary expressions");
    }
    if (CollectionUtils.isEmpty(logicalExpressions)) {
      throw new PlanException("Logical expressions cannot be empty while creating binary expressions");
    }
    LogicalExpression le1, le2;
    Iterator<LogicalExpression> it = logicalExpressions.iterator();
    le1 = it.next();
    for (; ; ) {
      if (!it.hasNext()) {
        break;
      }
      le2 = it.next();
      le1 = DFR.createExpression(binaryFunction, ExpressionPosition.UNKNOWN, le1, le2);
    }
    return le1;
  }

  private static LogicalExpression toBinaryExpression(Map<String, LogicalExpression> map, String binaryFunction) throws
    PlanException {
    if (MapUtils.isEmpty(map)) {
      return null;
    }
    Collection<LogicalExpression> logicalExpressions = new ArrayList<LogicalExpression>(map.size());

    LogicalExpression leftColumn, functionCall;
    for (Map.Entry<String, LogicalExpression> entry : map.entrySet()) {
      leftColumn = buildColumn(entry.getKey());
      functionCall = DFR.createExpression("==", ExpressionPosition.UNKNOWN, leftColumn, entry.getValue());
      logicalExpressions.add(functionCall);
    }

    return toBinaryExpression(logicalExpressions, binaryFunction);
  }

  public static JSONOptions getEventSelections(String projectId, XEvent originalXEvent, String realBeginDate,
                                               String realEndDate, String... additionalProjections) throws IOException,
    ParseException, TException, XEventException, MemCacheException, PlanException {

    // 拆解日期至单天, 保证每个selection都是连续的范围
    List<BeginEndDatePair> dates = DateSplitter.split2Pairs(realBeginDate, realEndDate, Interval.DAY);
    // 额外的投影运算, 根据外部需要什么投影决定
    NamedExpression[] projections;
    boolean emptyProjections = ArrayUtils.isEmpty(additionalProjections);
    if (emptyProjections) {
      projections = new NamedExpression[2];
    } else {
      projections = new NamedExpression[2 + additionalProjections.length];
    }
    FieldReference fr = buildColumn(KEY_WORD_UID);
    projections[0] = new NamedExpression(fr, fr);
    fr = buildColumn(KEY_WORD_VALUE);
    projections[1] = new NamedExpression(fr, fr);
    if (!emptyProjections) {
      for (int i = 0; i < additionalProjections.length; i++) {
        fr = buildColumn(additionalProjections[i]);
        projections[2 + i] = new NamedExpression(fr, fr);
      }
    }

    // 构建事件的filter-expressions
    Map<String, LogicalExpression> logicalExpressionMap;
    if (originalXEvent.isAll()) {
      logicalExpressionMap = new HashMap<String, LogicalExpression>(1);
    } else {
      logicalExpressionMap = new HashMap<String, LogicalExpression>(originalXEvent.maxLevel() + 1);
    }
    String[] arr = originalXEvent.getEventArray();
    for (int i = 0; i < arr.length; i++) {
      if (StringUtils.isNotBlank(arr[i])) {
        logicalExpressionMap.put(KEY_WORD_EVENT + i, new ValueExpressions.QuotedString(arr[i], UNKNOWN));
      }
    }

    String d;
    LogicalExpression le;
    ScanSelection ss;
    ScanSelection[] sss = new ScanSelection[dates.size()];
    ScanFilter sf;

    int cnt = 0;
    for (BeginEndDatePair datePair : dates) {
      d = datePair.getBeginDate();
      logicalExpressionMap.put("date", new ValueExpressions.QuotedString(d.replace("-", ""), UNKNOWN));

      sf = new ScanFilter(toBinaryExpression(logicalExpressionMap, "and"));
      ss = new ScanSelection(toEventTableName(projectId), sf, projections);
      sss[cnt] = ss;
      ++cnt;
    }

    return DEFAULT_DRILL_CONFIG.getMapper()
                               .readValue(DEFAULT_DRILL_CONFIG.getMapper().writeValueAsBytes(sss), JSONOptions.class);
  }

  public static PlanProperties buildPlanProperties(String projectId) {
    PlanProperties pp = new PlanProperties();
    pp.generator = new PlanProperties.Generator();
    pp.generator.info = projectId.trim();
    pp.generator.type = "AUTO";
    pp.version = 1;
    pp.type = PlanProperties.PlanType.APACHE_DRILL_LOGICAL;
    return pp;
  }

  public static Store getStore() {
    return new Store("DEFAULT-STORE", null, null);
  }

}
