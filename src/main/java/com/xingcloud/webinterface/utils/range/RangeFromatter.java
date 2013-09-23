package com.xingcloud.webinterface.utils.range;

import static com.xingcloud.basic.utils.DateUtils.date2Short;

import com.xingcloud.webinterface.enums.SliceType;

import java.util.Date;

public class RangeFromatter {

  public static String format(Object o, SliceType type) {
    if (type == null) {
      return o.toString();
    }
    switch (type) {
      case DATED:
        return date2Short(new Date(Long.parseLong(o.toString())));
      default:
        return o.toString();
    }
  }
}
