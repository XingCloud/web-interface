package com.xingcloud.webinterface.segment;

import static com.xingcloud.webinterface.segment.SegmentHandlerCreater.getHandlers;
import static com.xingcloud.webinterface.segment.SegmentSplitter.generateNewSegments;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.DEFAULT_GSON_PLAIN;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.TOTAL_USER;

import com.google.common.base.Strings;
import com.mongodb.BasicDBObject;
import com.mongodb.util.JSON;
import com.xingcloud.webinterface.exception.SegmentException;
import com.xingcloud.webinterface.model.formula.CommonFormulaQueryDescriptor;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class SegmentHandlerChain {

  private static boolean check1(String segment) throws SegmentException {
    if (Strings.isNullOrEmpty(segment) || TOTAL_USER.equals(segment)) {
      return false;
    }
    return true;
  }

  private static Object check2(String segment) throws SegmentException {
    Object o = JSON.parse(segment);
    if (!(o instanceof BasicDBObject)) {
      throw new SegmentException("SegmentPart cannot be cast to BasicDBObject - " + segment);
    }
    return o;
  }

  public static List<SegmentHandler> createHandlers(String segment) throws SegmentException {
    if (!check1(segment)) {
      return null;
    }
    Object o = check2(segment);
    return getHandlers((BasicDBObject) o);
  }

  public static void handleSegment(List<SegmentHandler> handlers, FormulaQueryDescriptor descriptor,
                                   boolean ignoreHandlers) throws SegmentException {
    if (descriptor == null) {
      return;
    }
    String segment = descriptor.getSegment();
    if (!check1(segment)) {
      return;
    }
    Object o = check2(segment);
    BasicDBObject dbo = (BasicDBObject) o;
    BasicDBObject newDBO = new BasicDBObject(dbo.size());
    for (Map.Entry<String, Object> entry : dbo.entrySet()) {
      newDBO.put(entry.getKey().toLowerCase(), entry.getValue());
    }

    // 完全不需要任何handler, 也不需要内部排序等等, 如{"a":"abc","b":123}
    if (CollectionUtils.isEmpty(handlers)) {
      descriptor.setSegment(DEFAULT_GSON_PLAIN.toJson(new TreeMap<Object, Object>(newDBO)));
      return;
    }

    // 需要使用Handler, 有3中情况
    // 1. 没有Handler关键字, 但某属性对应的值是DBObject, 因此需要重排序.
    // {"a":"abc","b":{"z":1,"y":2}}
    // 2. 有Handler关键字, 如{"a":"abc","b":{"$handler":"DateSplittor"}}
    // 3. 以上二者混合
    if (!ignoreHandlers) {
      for (SegmentHandler handler : handlers) {
        try {
          handler.handleSegment(newDBO, descriptor);
        } catch (Exception e) {
          throw new SegmentException(e.getMessage(), e);
        }
      }
    }

    descriptor.setSegment(DEFAULT_GSON_PLAIN.toJson(new TreeMap<Object, Object>(newDBO)));
  }

  public static void handleSegment(List<SegmentHandler> handlers, List<FormulaQueryDescriptor> descriptors,
                                   boolean ignoreHandlers) throws SegmentException {
    if (CollectionUtils.isEmpty(descriptors)) {
      return;
    }
    for (FormulaQueryDescriptor descriptor : descriptors) {
      handleSegment(handlers, descriptor, ignoreHandlers);
    }

  }

  public static void main(String[] args) throws Exception {

    String segment = "{\"identifier\":[\"a\",\"b\"],\"first_pay_time\":{\"$lte\":\"2013-01-14\",\"$gte\":\"2013-01-11\"}," + "\"last_login_time\": \"2012-08-10\",\"grade\": 10,\"register_time\":" + "{\"$handler\":\"DateSplittor\"}}";
    segment = "{\"register_time\":{\"$handler\":\"DateSplittor\",\"offset\":-1}}";
    // segment = Constants.TOTAL_USER;
    FormulaQueryDescriptor descriptor = new CommonFormulaQueryDescriptor();
    descriptor.setInputBeginDate("2012-04-01");
    descriptor.setInputEndDate("2012-04-03");
    descriptor.setRealBeginDate("2012-04-01");
    descriptor.setRealEndDate("2012-04-03");
    descriptor.setSegment(segment);

    Object[] result = generateNewSegments(segment);

    List<SegmentHandler> handlers = createHandlers(segment);
    handleSegment(handlers, descriptor, false);
    System.out.println(descriptor.getSegment());

    System.out.println("===================================");
    for (Object o : result) {
      handlers = createHandlers(o.toString());
      descriptor.setSegment(o.toString());
      handleSegment(handlers, descriptor, false);
      System.out.println(descriptor.getSegment());
    }
  }

}
