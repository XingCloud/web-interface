package com.xingcloud.webinterface.segment;

import static com.xingcloud.webinterface.utils.WebInterfaceConstants.DEFAULT_SEGMENT_HANDLER_NAME;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.HANDLER_KEYWORD;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.HANDLER_SUFFIX;

import com.mongodb.BasicDBObject;
import com.xingcloud.webinterface.annotation.JsonName;
import com.xingcloud.webinterface.exception.SegmentException;
import com.xingcloud.webinterface.utils.ReflectUtils;
import org.apache.commons.collections.MapUtils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

public class SegmentHandlerCreater {
  private static final String PACKAGE = SegmentHandlerCreater.class.getPackage().getName();

  public static SegmentHandler getHandler(Object k, Object v, Set<Object> existsHandlerNameSet) throws
    SegmentException {
    if (k == null || v == null) {
      return null;
    }
    if (!(v instanceof BasicDBObject)) {
      return null;
    }

    BasicDBObject dbo = (BasicDBObject) v;
    Object handlerName = dbo.get(HANDLER_KEYWORD);
    if (handlerName == null) {
      handlerName = DEFAULT_SEGMENT_HANDLER_NAME;
    }

    if (existsHandlerNameSet.contains(handlerName)) {
      return null;
    }
    existsHandlerNameSet.add(handlerName);

    Class<SegmentHandler> clz = null;
    SegmentHandler handlerInstance = null;
    String parameterName = null;
    Object parameterValue = null;
    JsonName anno = null;
    try {
      clz = (Class<SegmentHandler>) Class.forName(PACKAGE + "." + handlerName + HANDLER_SUFFIX);
      handlerInstance = ReflectUtils.newInstance(clz);

      if (MapUtils.isNotEmpty(dbo) && dbo.size() > 1) {
        Field[] fields = clz.getDeclaredFields();
        for (Field field : fields) {
          anno = field.getAnnotation(JsonName.class);
          if (anno == null) {
            parameterName = field.getName();
          } else {
            parameterName = anno.value();
          }
          parameterValue = dbo.get(parameterName);
          field.setAccessible(true);
          field.set(handlerInstance, parameterValue);
        }
      }
    } catch (Exception e) {
      throw new SegmentException(e);
    }

    return handlerInstance;
  }

  public static List<SegmentHandler> getHandlers(BasicDBObject segmentDBObject) throws SegmentException {
    if (MapUtils.isEmpty(segmentDBObject)) {
      return null;
    }
    List<SegmentHandler> handlers = null;
    SegmentHandler handler = null;
    Set<Object> existsHandlerNameSet = new HashSet<Object>();
    for (Entry<String, Object> entry : segmentDBObject.entrySet()) {
      handler = getHandler(entry.getKey(), entry.getValue(), existsHandlerNameSet);
      if (handler == null) {
        continue;
      }
      if (handlers == null) {
        handlers = new ArrayList<SegmentHandler>();
      }
      handlers.add(handler);
    }
    return handlers;
  }
}
