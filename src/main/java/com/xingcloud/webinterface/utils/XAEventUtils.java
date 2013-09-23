package com.xingcloud.webinterface.utils;

import com.xingcloud.webinterface.model.EventPart;

import java.util.ArrayList;
import java.util.List;

/**
 * User: Z J Wu Date: 13-8-5 Time: 下午3:00 Package: com.xingcloud.webinterface.utils
 */
public class XAEventUtils {

  public static List<EventPart> event2List(String event) {
    String[] eventArr = event.split("\\.");
    List<EventPart> list = new ArrayList<EventPart>(eventArr.length);
    for (byte b = 0; b < eventArr.length; b++) {
      if (!"*".equals(eventArr[b])) {
        list.add(new EventPart(eventArr[b], b));
      }
    }
    return list;
  }
}
