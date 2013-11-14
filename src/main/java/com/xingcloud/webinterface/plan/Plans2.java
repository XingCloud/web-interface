package com.xingcloud.webinterface.plan;

import static com.xingcloud.webinterface.enums.Operator.EQ;
import static com.xingcloud.webinterface.enums.Operator.GE;
import static com.xingcloud.webinterface.enums.Operator.GT;
import static com.xingcloud.webinterface.enums.Operator.LE;
import static com.xingcloud.webinterface.enums.Operator.SGMT300;
import static com.xingcloud.webinterface.enums.Operator.SGMT3600;
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

  public static final FunctionRegistry DFR = new FunctionRegistry(DrillConfig.create());

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
    return GE.equals(op) || GT.equals(op);
  }

  private static Object object2String(boolean transformDate, boolean beginSuffix, Object o) {
    if (o instanceof Object[]) {
      Object[] obj = (Object[]) o;
      if (ArrayUtils.isEmpty(obj)) {
        return null;
      }
      String separator = ((obj[0] instanceof String) ? "','" : ",");
      StringBuilder sb = new StringBuilder(obj[0].toString());
      for (int i = 1; i < obj.length; i++) {
        sb.append(separator);
        sb.append(obj[i]);
      }
      return sb.toString();
    } else if (o instanceof Number || o instanceof Boolean) {
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
    if (o instanceof Object[]) {
      return null;
    } else if (o instanceof String) {
      return new ValueExpressions.QuotedString(o.toString(), ExpressionPosition.UNKNOWN);
    } else if (o instanceof Number) {
      return ValueExpressions.getNumericExpression(o.toString(), ExpressionPosition.UNKNOWN);
    } else if (o instanceof Boolean) {
      return new ValueExpressions.BooleanExpression(o.toString(), ExpressionPosition.UNKNOWN);
    } else {
      return new FieldReference(o.toString(), ExpressionPosition.UNKNOWN);
    }
  }

  private static LogicalExpression toArrayLE(Collection collection) {
    Iterator it = collection.iterator();
    Object next = it.next();
    boolean pureString = (next instanceof String);
    String separator = (pureString ? "','" : ",");
    StringBuilder sb = new StringBuilder(next.toString());
    for (; ; ) {
      if (!it.hasNext()) {
        break;
      }
      next = it.next();
      sb.append(separator);
      sb.append(next.toString());
    }
    String str = sb.toString();
    LogicalExpression le = pureString ? new ValueExpressions.QuotedString(str, UNKNOWN) : buildColumn(str);
    return le;
  }

  private static LogicalOperator getSingleMysqlSegmentExprScan(String projectId, String propertyName,
                                                               Map<Operator, Object> valueMap) throws PlanException {
    if (MapUtils.isEmpty(valueMap)) {
      throw new PlanException("Empty segment value");
    }
    String table = toUserMysqlTableName(projectId, propertyName);
    boolean transformDate = usingDateFunc(projectId, propertyName), hasFunctionalSegment = valueMap
      .containsKey(SGMT300) || valueMap.containsKey(SGMT3600), hasEQ = valueMap.containsKey(EQ);
    Object valueObject = null;
    if (transformDate && (hasEQ || hasFunctionalSegment)) {
      if (hasEQ) {
        valueObject = valueMap.get(EQ);
        valueMap.remove(EQ);
      } else if (hasFunctionalSegment) {
        valueObject = valueMap.get(SGMT300);
        if (valueObject == null) {
          valueObject = valueMap.get(SGMT3600);
          valueMap.remove(SGMT3600);
        } else {
          valueMap.remove(SGMT300);
        }
      }
      valueMap.put(GE, valueObject);
      valueMap.put(LE, valueObject);
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
      sqlOperator = operator.getSqlOperator();
      usingBeginSuffix = usingBeginSuffix(operator);
      valueObject = entry.getValue();
      fieldValue = object2String(transformDate, usingBeginSuffix, valueObject);
      fieldValueReference = toLE(fieldValue);
      le = DFR.createExpression(sqlOperator, ExpressionPosition.UNKNOWN, fieldReference, fieldValueReference);
      logicalExpressions.add(le);
    }
    FieldReference fr = buildColumn(KEY_WORD_UID);
    NamedExpression[] projections;
    if (hasFunctionalSegment) {
      projections = new NamedExpression[2];
      projections[0] = new NamedExpression(fr, fr);
      projections[1] = new NamedExpression(buildColumn(field), buildColumn(propertyName));
    } else {
      projections = new NamedExpression[]{new NamedExpression(fr, fr)};
    }

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

  private static LogicalOperator getSingleMysqlSegmentExprScan2(String projectId, String propertyName,
                                                                Map<Operator, Object> valueMap) throws PlanException {
    if (MapUtils.isEmpty(valueMap)) {
      throw new PlanException("Empty segment value");
    }
    String table = toUserMysqlTableName(projectId, propertyName);
    boolean transformDate = usingDateFunc(projectId, propertyName), hasFunctionalSegment = valueMap
      .containsKey(SGMT300) || valueMap.containsKey(SGMT3600), hasEQ = valueMap.containsKey(EQ);
    Object valueObject = null;
    if (transformDate && (hasEQ || hasFunctionalSegment)) {
      if (hasEQ) {
        valueObject = valueMap.get(EQ);
        valueMap.remove(EQ);
      } else if (hasFunctionalSegment) {
        valueObject = valueMap.get(SGMT300);
        if (valueObject == null) {
          valueObject = valueMap.get(SGMT3600);
          valueMap.remove(SGMT3600);
        } else {
          valueMap.remove(SGMT300);
        }
      }
      valueMap.put(GE, valueObject);
      valueMap.put(LE, valueObject);
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
      sqlOperator = operator.getSqlOperator();
      usingBeginSuffix = usingBeginSuffix(operator);
      valueObject = entry.getValue();
      if (valueObject instanceof Collection) {
        fieldValueReference = toArrayLE((Collection) valueObject);
      } else {
        fieldValue = object2String(transformDate, usingBeginSuffix, valueObject);
        fieldValueReference = toLE(fieldValue);
      }
      le = DFR.createExpression(sqlOperator, ExpressionPosition.UNKNOWN, fieldReference, fieldValueReference);
      logicalExpressions.add(le);
    }
    FieldReference fr = buildColumn(KEY_WORD_UID);
    NamedExpression[] projections;
    if (hasFunctionalSegment) {
      projections = new NamedExpression[2];
      projections[0] = new NamedExpression(fr, fr);
      projections[1] = new NamedExpression(buildColumn(field), buildColumn(propertyName));
    } else {
      projections = new NamedExpression[]{new NamedExpression(fr, fr)};
    }

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
    LogicalOperator lo1 = getSingleMysqlSegmentExprScan(projectId, propertyName,
                                                        new HashMap<Operator, Object>(singleSegmentMap)), lo2;
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
      lo2 = getSingleMysqlSegmentExprScan(projectId, propertyName, new HashMap<Operator, Object>(singleSegmentMap));
      operators.add(lo2);

      joinConditions = new JoinCondition[1];
      joinConditions[0] = new JoinCondition("==", buildColumn(KEY_WORD_UID), buildColumn(KEY_WORD_UID));
      join = new Join(lo1, lo2, joinConditions, Join.JoinType.INNER);
      operators.add(join);
      lo1 = join;
    }
    return lo1;
  }

  public static LogicalOperator getChainedMysqlSegmentScan2(String projectId, List<LogicalOperator> operators,
                                                            Map<String, Map<Operator, Object>> segmentMap) throws
    PlanException {
    if (MapUtils.isEmpty(segmentMap)) {
      throw new PlanException("Empty segment");
    }
    Iterator<Map.Entry<String, Map<Operator, Object>>> it = segmentMap.entrySet().iterator();
    Map.Entry<String, Map<Operator, Object>> entry = it.next();
    String propertyName = entry.getKey();
    Map<Operator, Object> singleSegmentMap = entry.getValue();
    LogicalOperator lo1 = getSingleMysqlSegmentExprScan2(projectId, propertyName,
                                                         new HashMap<Operator, Object>(singleSegmentMap)), lo2;
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
      lo2 = getSingleMysqlSegmentExprScan2(projectId, propertyName, new HashMap<Operator, Object>(singleSegmentMap));
      operators.add(lo2);

      joinConditions = new JoinCondition[1];
      joinConditions[0] = new JoinCondition("==", buildColumn(KEY_WORD_UID), buildColumn(KEY_WORD_UID));
      join = new Join(lo1, lo2, joinConditions, Join.JoinType.INNER);
      operators.add(join);
      lo1 = join;
    }
    return lo1;
  }

  public static LogicalExpression getChainedSegmentFilter(Map<String, Map<Operator, Object>> segmentMap,
                                                          String rightFunc, String rightFuncColumn) throws
    PlanException {
    Collection<LogicalExpression> logicalExpressions = null;
    Map<Operator, Object> valueMap;
    Operator operator;
    String functionString;
    LogicalExpression left, right;
    for (Map.Entry<String, Map<Operator, Object>> entry1 : segmentMap.entrySet()) {
      valueMap = entry1.getValue();
      for (Map.Entry<Operator, Object> entry2 : valueMap.entrySet()) {
        operator = entry2.getKey();
        if (operator.isFunctional()) {
          functionString = operator.name().toLowerCase();
          left = DFR.createExpression(functionString, UNKNOWN, buildColumn(entry1.getKey()));
          right = DFR.createExpression(rightFunc, UNKNOWN, buildColumn(rightFuncColumn));
          left = DFR.createExpression("==", UNKNOWN, left, right);
          if (logicalExpressions == null) {
            logicalExpressions = new ArrayList<LogicalExpression>();
          }
          logicalExpressions.add(left);
        }
      }
    }
    return toBinaryExpression(logicalExpressions, "and");
  }

  public static LogicalExpression getChainedSegmentFilter2(Map<String, Operator> segmentMap, String rightFunc,
                                                           String rightFuncColumn) throws PlanException {
    Collection<LogicalExpression> logicalExpressions = null;
    Map<Operator, Object> valueMap;
    Operator operator;
    String functionString;
    LogicalExpression left, right;
    for (Map.Entry<String, Operator> entry : segmentMap.entrySet()) {
      operator = entry.getValue();
      if (operator.isFunctional()) {
        functionString = operator.name().toLowerCase();
        left = DFR.createExpression(functionString, UNKNOWN, buildColumn(entry.getKey()));
        right = DFR.createExpression(rightFunc, UNKNOWN, buildColumn(rightFuncColumn));
        left = DFR.createExpression("==", UNKNOWN, left, right);
        if (logicalExpressions == null) {
          logicalExpressions = new ArrayList<LogicalExpression>();
        }
        logicalExpressions.add(left);
      }
    }
    return toBinaryExpression(logicalExpressions, "and");
  }

  public static LogicalOperator getUserScan(String projectId, String groupBy) throws PlanException {
    JSONOptions selection;
    String table = toUserMysqlTableName(projectId, groupBy);
    List<Map<String, Object>> selectionMapList = new ArrayList<Map<String, Object>>(1);
    Map<String, Object> selectionMap = new HashMap<String, Object>(2);
    selectionMap.put("table", table);
    FieldReference uidFR = buildColumn(KEY_WORD_UID);
    NamedExpression[] projections = new NamedExpression[2];
    projections[0] = new NamedExpression(uidFR, uidFR);
    projections[1] = new NamedExpression(buildColumn(KEY_WORD_VAL), buildColumn(groupBy));

    selectionMap.put(KEY_WORD_PROJECTIONS, projections);
    selectionMapList.add(selectionMap);
    ObjectMapper mapper = DEFAULT_DRILL_CONFIG.getMapper();
    try {
      selection = mapper.readValue(mapper.writeValueAsString(selectionMapList), JSONOptions.class);
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

  public static LogicalExpression toBinaryExpression(Map<String, LogicalExpression> map, String binaryFunction) throws
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

  public static JoinCondition buildUidEQJoinCondition() {
    return new JoinCondition("==", buildColumn(KEY_WORD_UID), buildColumn(KEY_WORD_UID));
  }

}
