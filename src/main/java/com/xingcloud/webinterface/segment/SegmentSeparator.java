package com.xingcloud.webinterface.segment;

import static com.xingcloud.webinterface.enums.Operator.EQ;
import static com.xingcloud.webinterface.enums.Operator.IN;
import static com.xingcloud.webinterface.enums.SegmentExprType.CONST;
import static com.xingcloud.webinterface.utils.CartesianProduct.makeCartesianProduct;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.DEFAULT_GSON_PLAIN;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.SEGMENT_EXPR;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.SEGMENT_FUNCTION_BEGIN_CHAR;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.SEGMENT_IN_SEPARATOR;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.SEGMENT_OP;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.SEGMENT_TYPE;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.TOTAL_USER;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import com.xingcloud.webinterface.enums.Operator;
import com.xingcloud.webinterface.exception.SegmentException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class SegmentSeparator {

  public static String[] generateNewSegments(String segment) throws SegmentException {
    if (StringUtils.isBlank(segment) || TOTAL_USER.equals(segment)) {
      return new String[]{TOTAL_USER};
    }

    Map<String, String[]> m = new HashMap<String, String[]>();
    DBObject dbo = (DBObject) JSON.parse(segment);

    Map dboMap = dbo.toMap();
    Iterator it = dboMap.entrySet().iterator();
    Entry entry;
    BasicDBList dbList;
    BasicDBObject singleCondition;

    Operator operator;
    String expr;
    String[] slice;
    List<String> segments = new ArrayList<String>();

    Map<String, BasicDBList> noSeparateProperties = null;
    while (it.hasNext()) {
      entry = (Entry) it.next();
      dbList = (BasicDBList) entry.getValue();
      for (Object o : dbList) {
        singleCondition = (BasicDBObject) o;
        operator = Enum.valueOf(Operator.class, singleCondition.getString(SEGMENT_OP).toUpperCase());
        if (!IN.equals(operator)) {
          if (noSeparateProperties == null) {
            noSeparateProperties = new HashMap<String, BasicDBList>();
          }
          noSeparateProperties.put(entry.getKey().toString(), dbList);
          break;
        }
        expr = singleCondition.getString(SEGMENT_EXPR);
        slice = expr.split(SEGMENT_IN_SEPARATOR);
        m.put(entry.getKey().toString(), slice);
      }
    }
    List<Map<String, Object>> l = makeCartesianProduct(m);
    Map mmm;
    Object obj;
    if (CollectionUtils.isEmpty(l)) {
      segments.add(DEFAULT_GSON_PLAIN.toJson(noSeparateProperties));
    } else {
      for (Map<String, Object> map : l) {
        mmm = new HashMap(dboMap);
        for (Entry<String, Object> entry1 : map.entrySet()) {
          dbList = new BasicDBList();
          singleCondition = new BasicDBObject();
          singleCondition.put(SEGMENT_OP, EQ.name().toLowerCase());
          obj = entry1.getValue();

          singleCondition.put(SEGMENT_EXPR, obj);
          if (obj instanceof String) {
            expr = obj.toString();
            if (StringUtils.contains(expr, SEGMENT_FUNCTION_BEGIN_CHAR)) {
              throw new SegmentException("Expression \"" + expr +
                                           "\" found in \"IN\" operator - " + segment);
//              singleCondition.put(SEGMENT_TYPE, VAR.name());
//              dbList.add(singleCondition);
//              mmm.put(entry1.getKey(), dbList);
//              continue;
            }
          }
          singleCondition.put(SEGMENT_TYPE, CONST.name());
          dbList.add(singleCondition);
          mmm.put(entry1.getKey(), dbList);
        }
        if (MapUtils.isNotEmpty(noSeparateProperties)) {
          mmm.putAll(noSeparateProperties);
        }
        segments.add(DEFAULT_GSON_PLAIN.toJson(mmm));
      }
    }

    return segments.toArray(new String[segments.size()]);
  }

  public static void main(String[] args) throws SegmentException {
    String complex = "{\"register_time\":[{\"op\":\"ge\",\"expr\":\"$date_add(`start_date`, 3)\",\"type\":\"VAR\"},{\"op\":\"le\",\"expr\":\"$date_add(`end_date`, 5)\",\"type\":\"VAR\"}],\"grade\":[{\"op\":\"eq\",\"expr\":10,\"type\":\"CONST\"}],\"last_login_time\":[{\"op\":\"ge\",\"expr\":\"2013-05-01\",\"type\":\"CONST\"},{\"op\":\"lt\",\"expr\":\"2013-05-09\",\"type\":\"CONST\"}],\"first_pay_time\":[{\"op\":\"eq\",\"expr\":\"$date_add(`start_date`, 3)\",\"type\":\"VAR\"}],\"identifier\":[{\"op\":\"in\",\"expr\":\"a|b|c\",\"type\":\"CONST\"}],\"language\":[{\"op\":\"in\",\"expr\":\"en_us|zh_cn\",\"type\":\"CONST\"}],\"last_pay_time\":[{\"op\":\"in\",\"expr\":\"$date_add(`start_date`, 1)|$date_add(`start_date`, 2)|2013-05-13\",\"type\":\"MIX\"}]}";
    String simple1 = "{\"identifier\":[{\"op\": \"in\", \"expr\": \"a|b|c\", \"type\": \"CONST\"}]}";
    String simple2 = "{\"last_pay_time\":[{\"op\": \"in\", \"expr\": \"$date_add(`start_date`, 1)|$date_add(`start_date`, 2)|2013-05-13\", \"type\": \"MIX\"}]}";
    String simple3 = "{\"register_time\": [{\"op\": \"ge\", \"expr\": \"$date_add(`start_date`, 3)\", \"type\": \"VAR\"},{\"op\": \"le\", \"expr\": \"2013-05-13\", \"type\": \"CONST\"}]}";
    String simple4 = "{\"register_time\": [{\"op\": \"ge\", \"expr\": \"$date_add(`start_date`, 3)\", \"type\": \"VAR\"},{\"op\": \"le\", \"expr\": \"2013-05-13\", \"type\": \"CONST\"}]}";
    String[] segments;
    segments = generateNewSegments(simple1);
    for (String s : segments) {
      System.out.println(s);
    }
    System.out.println("---------------------------------");

    segments = generateNewSegments(simple3);
    for (String s : segments) {
      System.out.println(s);
    }
    System.out.println("---------------------------------");

    segments = generateNewSegments(simple2);
    for (String s : segments) {
      System.out.println(s);
    }
    System.out.println("---------------------------------");

    segments = generateNewSegments(complex);
    for (String s : segments) {
      System.out.println(s);
    }

    System.out.println("---------------------------------");
    segments = generateNewSegments(simple4);
    for (String s : segments) {
      System.out.println(s);
    }

  }
}
