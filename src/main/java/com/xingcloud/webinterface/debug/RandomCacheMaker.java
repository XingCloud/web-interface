package com.xingcloud.webinterface.debug;

import static com.xingcloud.basic.Constants.DEFAULT_TIME_ZONE;
import static com.xingcloud.webinterface.enums.Interval.PERIOD;
import static com.xingcloud.webinterface.utils.WebInterfaceRandomUtils.randomTuple;

import com.xingcloud.webinterface.enums.CacheReference;
import com.xingcloud.webinterface.enums.CacheState;
import com.xingcloud.webinterface.enums.Interval;
import com.xingcloud.webinterface.model.ResultTuple;
import com.xingcloud.webinterface.model.StatefulCache;
import com.xingcloud.webinterface.model.formula.CommonFormulaQueryDescriptor;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class RandomCacheMaker {

  public static StatefulCache randomCache(FormulaQueryDescriptor fqd) throws ParseException {
    if (fqd == null) {
      return null;
    }

    Map<Object, ResultTuple> tupleMap;
    if (fqd instanceof CommonFormulaQueryDescriptor) {
      CommonFormulaQueryDescriptor cfqd = (CommonFormulaQueryDescriptor) fqd;

      Calendar c = Calendar.getInstance(DEFAULT_TIME_ZONE);
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
      SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm");
      sdf.setTimeZone(DEFAULT_TIME_ZONE);
      sdf2.setTimeZone(DEFAULT_TIME_ZONE);
      Date d = sdf.parse(fqd.getRealBeginDate());
      c.setTime(d);

      Interval interval = cfqd.getInterval();
      switch (interval) {
        case MIN5:
          tupleMap = new HashMap<Object, ResultTuple>(288);
          for (int i = 0; i < 288; i++) {
            tupleMap.put(sdf2.format(c.getTime()), randomTuple());
            c.add(Calendar.MINUTE, 5);
          }
          break;
        case HOUR:
          tupleMap = new HashMap<Object, ResultTuple>(24);
          for (int i = 0; i < 24; i++) {
            tupleMap.put(sdf2.format(c.getTime()), randomTuple());
            c.add(Calendar.HOUR, 1);
          }
          break;
        default:
          tupleMap = new HashMap<Object, ResultTuple>(1);
          tupleMap.put(PERIOD.name(), randomTuple());
          break;
      }
    } else {
      int cnt = 500;
      tupleMap = new HashMap<Object, ResultTuple>(cnt);
      Random random = new Random();
      for (int i = 0; i < cnt; i++) {
//        tupleMap.put(date2Short(randomDate(today(), 5)), randomTuple());
        tupleMap.put(String.valueOf(random.nextInt(2000)), randomTuple());
      }
    }
    StatefulCache sc = new StatefulCache(CacheReference.OFFLINE, CacheState.ACCURATE, tupleMap, 0);
    return sc;
  }
}
