package com.xingcloud.webinterface.utils;

import static com.xingcloud.basic.Constants.DEFAULT_TIME_ZONE;
import static com.xingcloud.basic.utils.DateUtils.today;

import com.xingcloud.basic.utils.DateUtils;
import com.xingcloud.webinterface.enums.Interval;
import com.xingcloud.webinterface.model.ResultTuple;
import com.xingcloud.webinterface.model.formula.CommonFormulaQueryDescriptor;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import org.apache.commons.lang.math.RandomUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class WebInterfaceRandomUtils {

  public static Date randomDate(int seed) {
    return randomDate(null, seed);
  }

  public static Date randomDate(Date targetDate, int seed) {
    if (targetDate == null) {
      targetDate = today();
    }
    if (seed == 0) {
      seed = 10;
    }
    long l = Math.round(Math.random() * seed) * (RandomUtils.nextBoolean() ? 1 : -1
    ) * 3600 * 24 * 1000;
    return new Date(targetDate.getTime() + l);
  }

  public static Map<Object, ResultTuple> randomTuple(FormulaQueryDescriptor fqd) throws ParseException {
    Map<Object, ResultTuple> map = null;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    sdf.setTimeZone(DEFAULT_TIME_ZONE);
    sdf2.setTimeZone(DEFAULT_TIME_ZONE);

    Date d = sdf.parse(fqd.getRealBeginDate());
    Calendar c = Calendar.getInstance(DEFAULT_TIME_ZONE);
    c.setTime(d);

    boolean needCount = fqd.isNeedCountFunction();
    boolean needSum = fqd.isNeedSumFunction();
    boolean needUserNum = fqd.isNeedUserNumFunction();

    if (fqd instanceof CommonFormulaQueryDescriptor) {
      Interval interval = ((CommonFormulaQueryDescriptor) fqd).getInterval();
      if (Interval.MIN5.equals(interval)) {
        map = new HashMap<Object, ResultTuple>(288);
        for (int i = 0; i < 288; i++) {
          map.put(sdf2.format(c.getTime()), randomTuple(needCount, needSum, needUserNum));
          c.add(Calendar.MINUTE, 5);
        }
      } else if (Interval.HOUR.equals(interval)) {
        map = new HashMap<Object, ResultTuple>(24);
        for (int i = 0; i < 24; i++) {
          map.put(sdf2.format(c.getTime()), randomTuple(needCount, needSum, needUserNum));
          c.add(Calendar.HOUR, 1);
        }
      } else {
        map = new HashMap<Object, ResultTuple>(1);
        map.put(fqd.getRealBeginDate() + " 00:00", randomTuple(needCount, needSum, needUserNum));
      }
    } else {
      map = new HashMap<Object, ResultTuple>(1);
      for (int i = 0; i < 10; i++) {
        map.put(WebInterfaceCommonUtils.randomLong(100) + "", randomTuple(needCount, needSum, needUserNum));
      }
    }

    return map;
  }

  public static ResultTuple randomTuple(boolean needCount, boolean needSum, boolean needUserNum) {
    Long count = needCount ? Math.round(Math.random() * 10000) : null;
    Long sum = needSum ? Math.round(Math.random() * 10000) : null;
    Long userNum = needUserNum ? Math.round(Math.random() * 10000) : null;
    return new ResultTuple(count, sum, userNum);
  }

  public static ResultTuple randomTuple() {
    Long count = Math.round(Math.random() * 10000);
    Long sum = Math.round(Math.random() * 10000);
    Long userNum = Math.round(Math.random() * 10000);
    return new ResultTuple(count, sum, userNum);
  }

  public static void main(String[] args) {
    for (int i = 0; i < 10; i++) {
      System.out.println(DateUtils.date2Short(randomDate(today(), 5)));
    }
  }
}
