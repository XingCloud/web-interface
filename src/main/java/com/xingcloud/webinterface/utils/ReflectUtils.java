package com.xingcloud.webinterface.utils;

import com.xingcloud.webinterface.annotation.Ignore;
import com.xingcloud.webinterface.exception.RefelectException;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReflectUtils {
  private static final Class<?>[] EMPTY_ARRAY = new Class[]{};

  public static <T> T newInstance(Class<T> theClass) throws Exception {
    T result;
    Constructor<T> constructor;
    // synchronized (ReflectUtils.class) {
    // constructor = (Constructor<T>) CONSTRUCTOR_CACHE.get(theClass
    // .toString());
    // if (constructor == null) {
    constructor = theClass.getDeclaredConstructor(EMPTY_ARRAY);
    constructor.setAccessible(true);
    // CONSTRUCTOR_CACHE.put(theClass.toString(), constructor);
    // }
    // }

    result = constructor.newInstance();
    return result;
  }

  public static Map<String, Object> getDeclaredFieldNameValueMap(Class<?> clz, Object obj) throws RefelectException {

    Map<String, Object> map = new HashMap<String, Object>();
    getDeclaredFieldNameValueMap(clz, obj, map);
    return map;
  }

  public static void getDeclaredFieldNameValueMap(Class<?> clz, Object obj, Map<String, Object> map) throws
    RefelectException {
    if (clz == null || map == null) {
      return;
    }

    Field[] fields = clz.getDeclaredFields();
    String fieldName;
    Object fieldValue;
    Ignore ignoreAnnotation;
    boolean ignore;
    try {
      for (Field field : fields) {
        field.setAccessible(true);
        ignoreAnnotation = field.getAnnotation(Ignore.class);
        ignore = (ignoreAnnotation != null && ignoreAnnotation.value());
        if (!ignore) {
          fieldValue = field.get(obj);
          if (fieldValue == null) {
            continue;
          }
          fieldName = field.getName();
          // 处理枚举
          if (fieldValue instanceof Enum) {
            map.put(fieldName, ((Enum<?>) fieldValue).name());
          }
          // 处理基本类型
          else if (fieldValue instanceof Boolean || fieldValue instanceof String || fieldValue instanceof Number) {
            map.put(fieldName, fieldValue);
          }
          // 处理集合和数组
          else if (fieldValue.getClass().isArray()) {
            int length = Array.getLength(fieldValue);
            List<Object> list = new ArrayList<Object>(length);
            for (int i = 0; i < length; i++) {
              list.add(Array.get(fieldValue, i));
            }
            map.put(fieldName, list);
          } else if (fieldValue instanceof Collection) {
            map.put(fieldName, fieldValue);
          } else if (fieldValue instanceof Map) {
            map.put(fieldName, fieldValue);
          }
          // 处理复杂类型
          else {
            map.put(fieldName, getDeclaredFieldNameValueMap(fieldValue.getClass(), fieldValue));
          }
        }
      }
    } catch (Exception e) {
      throw new RefelectException(e.getMessage(), e);
    }
  }

  public static <T> Object getFieldValue(Class<T> clz, T t, String fieldName) throws RefelectException {
    try {
      Field field = clz.getDeclaredField(fieldName);
      field.setAccessible(true);
      return field.get(t);
    } catch (Exception e) {
      throw new RefelectException(e);
    }
  }

}
