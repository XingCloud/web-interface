package com.xingcloud.webinterface.segment;

import static com.xingcloud.webinterface.calculate.func.DateAddFunction.DATE_ADD_FUNCTION_NAME;
import static com.xingcloud.webinterface.enums.Interval.HOUR;
import static com.xingcloud.webinterface.enums.Interval.MIN5;
import static com.xingcloud.webinterface.enums.Operator.EQ;
import static com.xingcloud.webinterface.enums.Operator.GT;
import static com.xingcloud.webinterface.enums.Operator.GTE;
import static com.xingcloud.webinterface.enums.Operator.IN;
import static com.xingcloud.webinterface.enums.Operator.LT;
import static com.xingcloud.webinterface.enums.Operator.LTE;
import static com.xingcloud.webinterface.enums.Operator.SGMT300;
import static com.xingcloud.webinterface.enums.Operator.SGMT3600;
import static com.xingcloud.webinterface.enums.SegmentExprType.CONST;
import static com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor.JSON_NAME_FIELD_NAME_MAP;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.DEFAULT_GSON_PLAIN;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.SEGMENT_EXPR;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.SEGMENT_OP;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.SEGMENT_TYPE;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.SEGMENT_VARIABLE_BOUNDARY_CHAR;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.TOTAL_USER;
import static org.apache.commons.beanutils.PropertyUtils.getProperty;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import com.xingcloud.webinterface.calculate.Arity;
import com.xingcloud.webinterface.calculate.Evaluator;
import com.xingcloud.webinterface.enums.CommonQueryType;
import com.xingcloud.webinterface.enums.Interval;
import com.xingcloud.webinterface.enums.Operator;
import com.xingcloud.webinterface.enums.SegmentExprType;
import com.xingcloud.webinterface.exception.ExpressionEvaluationException;
import com.xingcloud.webinterface.exception.SegmentException;
import com.xingcloud.webinterface.model.Filter;
import com.xingcloud.webinterface.model.formula.CommonFormulaQueryDescriptor;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * User: Z J Wu Date: 13-5-8 Time: 下午6:06 Package: com.xingcloud.webinterface.segment
 */
public class SegmentEvaluator {

  private static String[] findVariables(String expr) throws SegmentException {

    int pointer = 0;
    String[] variables = null;
    int begin;
    int end;
    String variable;
    while (true) {
      begin = expr.indexOf(SEGMENT_VARIABLE_BOUNDARY_CHAR, pointer);
      if (begin < 0) {
        break;
      }
      end = expr.indexOf(SEGMENT_VARIABLE_BOUNDARY_CHAR, begin + 1);
      if (end < 0) {
        throw new SegmentException("Odd boundary chars found");
      }
      variable = expr.substring(begin + 1, end);
      variables = ArrayUtils.add(variables, variable);
      pointer = end + 1;
    }

    return variables;
  }

  private static String findOffset(String expr) {
    int a, b;
    a = expr.indexOf('(');
    b = expr.indexOf(')');
    return expr.substring(a + 1, b);
  }

  private static Object variable2Segment(Operator operator, String expr, FormulaQueryDescriptor descriptor) throws
    SegmentException, IllegalAccessException, NoSuchMethodException, InvocationTargetException,
    ExpressionEvaluationException {
    String[] variables = findVariables(expr);
    if (ArrayUtils.isEmpty(variables)) {
      throw new SegmentException("Cannot find any variable.");
    }
    expr = expr.replace(String.valueOf(SEGMENT_VARIABLE_BOUNDARY_CHAR), "");
    List<Arity> arityList = new ArrayList<Arity>(variables.length);

    Object fieldValue;
    for (String variable : variables) {
      fieldValue = getProperty(descriptor, JSON_NAME_FIELD_NAME_MAP.get(variable));
      arityList.add(new Arity(variable, fieldValue));
    }
    Object val = Evaluator.evaluateObject(expr, arityList);

    if (EQ.equals(operator)) {
      return val;
    }

    Map<String, Object> map = new HashMap<String, Object>(1);
    map.put(operator.getMongoKeyword(), val);
    return map;
  }

  private static Map<String, Object> variable2Segment2(Operator operator, String expr,
                                                       FormulaQueryDescriptor descriptor) throws SegmentException,
    IllegalAccessException, NoSuchMethodException, InvocationTargetException, ExpressionEvaluationException {
    String offset = findOffset(expr);

    String variable;
    if (GT.equals(operator) || GTE.equals(operator)) {
      variable = "start_date";
    } else if (LT.equals(operator) || LTE.equals(operator)) {
      variable = "end_date";
    } else {
      throw new SegmentException(operator + " is not supported in expression " + expr);
    }
    expr = "$date_add(" + variable + ", " + offset + ")";
    List<Arity> arityList = new ArrayList<Arity>(1);

    Object fieldValue = getProperty(descriptor, JSON_NAME_FIELD_NAME_MAP.get(variable));
    arityList.add(new Arity(variable, fieldValue));
    Object val = Evaluator.evaluateObject(expr, arityList);

    Map<String, Object> map = new HashMap<String, Object>(1);
    map.put(operator.getMongoKeyword(), val);
    return map;
  }

  private static Object const2Segment(Operator operator, Object val) {
    String toString = val.toString();
    if (toString.contains(DATE_ADD_FUNCTION_NAME)) {
      Map<String, Object> map = new TreeMap<String, Object>();
      map.put("$handler", "DateSplittor");
      int a, b;
      a = toString.indexOf('(');
      b = toString.indexOf(')');
      if (a < 0 || b < 0 || a >= b) {
        map.put("offset", 0);
      } else {
        String str = toString.substring(a + 1, b);
        map.put("offset", Integer.valueOf(str));
      }
      return map;
    }
    if (EQ.equals(operator)) {
      return val;
    }
    Map<String, Object> map = new TreeMap<String, Object>();
    map.put(operator.getMongoKeyword(), val);
    return map;
  }

  public static void evaluate(Collection<FormulaQueryDescriptor> descriptors) throws SegmentException {
    if (CollectionUtils.isEmpty(descriptors)) {
      return;
    }
    for (FormulaQueryDescriptor descriptor : descriptors) {
      evaluate(descriptor);
    }
  }

  public static void evaluate(FormulaQueryDescriptor descriptor) throws SegmentException {
    try {
      _evaluate(descriptor);
    } catch (SegmentException e) {
      throw e;
    } catch (Exception e) {
      throw new SegmentException(e);
    }
  }

  private static void _evaluate(FormulaQueryDescriptor descriptor) throws SegmentException, IllegalAccessException,
    NoSuchMethodException, InvocationTargetException, ExpressionEvaluationException {
    if (descriptor == null) {
      return;
    }
    String segment = descriptor.getSegment();
    if (StringUtils.isBlank(segment) || TOTAL_USER.equals(segment)) {
      descriptor.setSegment(TOTAL_USER);
      descriptor.setSegmentMap(null);
      return;
    }
    Operator op = null;
    boolean ignoreVar = false;
    if (descriptor.isCommon()) {
      Interval interval = ((CommonFormulaQueryDescriptor) descriptor).getInterval();
      float days = interval.getDays();
      if (days < 1) {
        ignoreVar = true;
      }

      if (HOUR.equals(interval)) {
        op = SGMT3600;
      } else if (MIN5.equals(interval)) {
        op = SGMT300;
      }
    }

    DBObject segmentDBO = (DBObject) JSON.parse(segment);
    Map m = segmentDBO.toMap();
    Map.Entry e;
    String userProperty;
    BasicDBList dbl;

    Iterator it;
    BasicDBObject dbo;

    Operator operator;
    SegmentExprType type;
    Object expression, singleSegment, val;
    Map<String, Map<Operator, Object>> segmentMap = new HashMap<String, Map<Operator, Object>>(m.size());
    Map<Operator, Object> singleSegmentMap;
    Map<String, Object> map, sortedSegmentMap = new TreeMap<String, Object>();

    for (Object o : m.entrySet()) {
      e = (Map.Entry) o;
      userProperty = ((String) e.getKey()).toLowerCase();
      dbl = (BasicDBList) e.getValue();
      it = dbl.iterator();
      singleSegmentMap = new HashMap<Operator, Object>(dbl.size());
      while (it.hasNext()) {
        dbo = (BasicDBObject) it.next();
        operator = Enum.valueOf(Operator.class, dbo.getString(SEGMENT_OP).toUpperCase());
        if (IN.equals(operator)) {
          throw new SegmentException("\"IN\" operator must be split to small piece before transform.");
        }
        type = Enum.valueOf(SegmentExprType.class, dbo.getString(SEGMENT_TYPE).toUpperCase());
        expression = dbo.get(SEGMENT_EXPR);
        // 常量型表达式, 5分钟/小时的date_handler直接返回
        if (ignoreVar || CONST.equals(type)) {
          singleSegment = const2Segment(operator, expression);
        }
        // 变量型表达式需要经过Evaluator计算
        else {
          singleSegment = variable2Segment2(operator, expression.toString(), descriptor);
        }
        if (singleSegment instanceof Map) {
          map = (Map<String, Object>) sortedSegmentMap.get(userProperty);
          if (map == null) {
            map = new HashMap<String, Object>();
            sortedSegmentMap.put(userProperty, map);
          }
          map.putAll((Map) singleSegment);
          val = ((Map) singleSegment).get(operator.getMongoKeyword());
          if (val == null) {
            singleSegmentMap.put(op, descriptor.getRealBeginDate());
            if (descriptor.isCommon()) {
              ((CommonFormulaQueryDescriptor) descriptor).setFunctionalSegment(true);
            }
          } else {
            singleSegmentMap.put(operator, val);
          }
        } else {
          sortedSegmentMap.put(userProperty, singleSegment);
          singleSegmentMap.put(operator, singleSegment);
        }
      }
      segmentMap.put(userProperty, singleSegmentMap);
    }
    descriptor.setSegmentMap(segmentMap);
    descriptor.setSegment(DEFAULT_GSON_PLAIN.toJson(sortedSegmentMap));
  }

  public static void main(String[] args) throws SegmentException, IllegalAccessException, NoSuchMethodException,
    InvocationTargetException {
    String segment = "{\"register_time\":[{\"op\":\"ge\",\"expr\":\"$date_add(3)\",\"type\":\"VAR\"},{\"op\":\"le\",\"expr\":\"$date_add(`end_date`, 5)\",\"type\":\"VAR\"}],\"grade\":[{\"op\":\"eq\",\"expr\":10,\"type\":\"CONST\"}]}";
    segment = "{\"nation\":[{\"op\":\"eq\",\"expr\":\"zh\",\"type\":\"CONST\"}],\"register_time\":[{\"op\":\"gte\",\"expr\":\"$date_add(1)\",\"type\":\"VAR\"},{\"op\":\"lte\",\"expr\":\"$date_add(1)\",\"type\":\"VAR\"}]}";
    FormulaQueryDescriptor fqd = new CommonFormulaQueryDescriptor("age", "2013-03-15", "2013-03-15", "visit.*", segment,
                                                                  Filter.ALL, "2013-03-10", "2013-03-12", Interval.MIN5,
                                                                  CommonQueryType.NORMAL);
    evaluate(fqd);
    System.out.println(fqd.getSegment());
    System.out.println(fqd.getSegmentMap());
//    System.out.println("---------------------------------");
//
//    segment = "{\"nation\":[{\"op\":\"eq\",\"expr\":\"zh\",\"type\":\"CONST\"}],\"register_time\":[{\"op\":\"eq\",\"expr\":\"2013-07-13\",\"type\":\"CONST\"}]}";
//    fqd = new CommonFormulaQueryDescriptor("age", "2013-03-15", "2013-03-15", "visit.*", segment, Filter.ALL, 1d,
//                                           "2013-03-10", "2013-03-12", Interval.HOUR, CommonQueryType.NORMAL);
////    segment = "{\"identifier\":[{\"op\":\"eq\",\"expr\":\"2013-06-28\",\"type\":\"CONST\"}]}";
//    evaluate(fqd);
//    System.out.println(fqd.getSegment());
//    System.out.println(fqd.getSegmentMap());
//    System.out.println("---------------------------------");  0
//
//    fqd = new CommonFormulaQueryDescriptor("age", "2013-03-15", "2013-03-15", "visit.*", segment, Filter.ALL, 1d,
//                                           "2013-03-10", "2013-03-12", Interval.PERIOD, CommonQueryType.NORMAL);
//
//    evaluate(fqd);
//    System.out.println(fqd.getSegment());
//    System.out.println(fqd.getSegmentMap());
  }
}
