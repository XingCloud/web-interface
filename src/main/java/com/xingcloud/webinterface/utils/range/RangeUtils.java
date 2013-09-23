package com.xingcloud.webinterface.utils.range;

import static com.google.common.collect.Range.all;
import static com.google.common.collect.Range.atLeast;
import static com.google.common.collect.Range.atMost;
import static com.google.common.collect.Range.closed;
import static com.google.common.collect.Range.greaterThan;
import static com.google.common.collect.Range.lessThan;
import static com.google.common.collect.Range.singleton;
import static com.xingcloud.basic.utils.DateUtils.dayDistance;
import static com.xingcloud.basic.utils.DateUtils.previousDay;
import static com.xingcloud.basic.utils.DateUtils.short2Date;

import com.google.common.base.Strings;
import com.google.common.collect.Range;
import com.xingcloud.webinterface.enums.Operator;
import com.xingcloud.webinterface.enums.SliceType;
import com.xingcloud.webinterface.exception.RangingException;
import com.xingcloud.webinterface.exception.WrongRangeException;
import com.xingcloud.webinterface.exception.XQueryException;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

public class RangeUtils {

  private static final Logger LOGGER = Logger.getLogger(RangeUtils.class);

  private static Range<Long> short2RangeLong(Operator operator, String date) {
    String s = date.replace("-", "");
    Long l = Long.valueOf(s);
    switch (operator) {
      case GT:
        return greaterThan(l);
      case GTE:
        return atLeast(l);
      case LT:
        return lessThan(l);
      case LTE:
        return atMost(l);
      case EQ:
        return singleton(l);
      default:
        return all();
    }
  }

  private static Range<Long> short2RangeLong(String operatorString, String date) {
    String s = date.replace("-", "");
    Long l = Long.valueOf(s);
    Operator operator = Operator.OPERATOR_MONGO_KEYWORDS_BIMAP.inverse().get(operatorString);
    switch (operator) {
      case GT:
        return greaterThan(l);
      case GTE:
        return atLeast(l);
      case LT:
        return lessThan(l);
      case LTE:
        return atMost(l);
      case EQ:
        return singleton(l);
      default:
        return all();
    }
  }

  public static Range<Long> continuouslyIntersect(Collection<Range<Long>> ranges) throws WrongRangeException {
    Iterator<Range<Long>> it = ranges.iterator();
    if (!it.hasNext()) {
      return null;
    }
    Range<Long> resultRange = it.next();
    Range<Long> next = null;

    try {
      while (it.hasNext()) {
        next = it.next();
        resultRange = resultRange.intersection(next);
      }
    } catch (Exception e) {
      throw new WrongRangeException(e);
    }

    return resultRange;
  }

  public static Collection<XRange<Long>> getXRangeFromDate(String pattern, SliceType sliceType) throws
    RangingException {
    if (Strings.isNullOrEmpty(pattern)) {
      return null;
    }
    String[] patternStringArray = pattern.split(",");
    if (ArrayUtils.isEmpty(patternStringArray)) {
      return null;
    }
    int length = patternStringArray.length;
    long[] patternDate = new long[length];
    String tmp = null;
    try {
      for (int i = 0; i < length; i++) {
        tmp = StringUtils.trimToNull(patternStringArray[i]);
        patternDate[i] = short2Date(tmp).getTime();
      }
    } catch (ParseException e) {
      throw new RangingException("Cannot convert slice pattern string to date - " + tmp, e);
    }
    Arrays.sort(patternDate);
    Collection<XRange<Long>> ranges = new ArrayList<XRange<Long>>(length + 1);
    int position = 0;
    ranges.add(new XRange<Long>(lessThan(patternDate[0]), position, sliceType));

    for (int i = 0; i < length - 1; i++) {
      position = i + 1;
      if (dayDistance(patternDate[i + 1], patternDate[i]) <= 1) {
        ranges.add(new XRange<Long>(singleton(patternDate[i]), position, sliceType));
      } else {
        ranges.add(new XRange<Long>(closed(patternDate[i], previousDay(patternDate[i + 1])), position, sliceType));
      }
    }

    ranges.add(new XRange<Long>(atLeast(patternDate[length - 1]), length, sliceType));
    return ranges;
  }

  public static Collection<XRange<Long>> getXRangeFromLong(String pattern, SliceType sliceType) throws
    RangingException {
    if (Strings.isNullOrEmpty(pattern)) {
      return null;
    }
    String[] patternStringArray = pattern.split(",");
    if (ArrayUtils.isEmpty(patternStringArray)) {
      return null;
    }
    int length = patternStringArray.length;
    long[] patternLong = new long[length];

    String tmp = null;
    try {
      for (int i = 0; i < length; i++) {
        tmp = StringUtils.trimToNull(patternStringArray[i]);
        patternLong[i] = Long.parseLong(tmp);
      }
    } catch (Exception e) {
      throw new RangingException("Cannot convert slice pattern string to long - " + tmp, e);
    }
    Arrays.sort(patternLong);
    Collection<XRange<Long>> ranges = new ArrayList<XRange<Long>>(length + 1);

    int position = 0;
    ranges.add(new XRange<Long>(lessThan(patternLong[0]), position, sliceType));

    for (int i = 0; i < length - 1; i++) {
      position = i + 1;
      if ((patternLong[i + 1] - patternLong[i]) <= 1) {
        ranges.add(new XRange<Long>(singleton(patternLong[i]), position, sliceType));
      } else {
        ranges.add(new XRange<Long>(closed(patternLong[i], patternLong[i + 1] - 1), position, sliceType));
      }
    }
    ranges.add(new XRange<Long>(atLeast(patternLong[length - 1]), length, sliceType));
    return ranges;
  }

  public static void main(String[] args) throws XQueryException, RangingException {
    Collection<XRange<Long>> ranges = null;
    System.out.println("===================================");
    ranges = getXRangeFromLong("0", SliceType.NUMERIC);
    System.out.println(ranges);

    System.out.println("===================================");
    ranges = getXRangeFromLong("0,1", SliceType.NUMERIC);
    System.out.println(ranges);

    System.out.println("===================================");
    ranges = getXRangeFromLong("0,10", SliceType.NUMERIC);
    System.out.println(ranges);

    System.out.println("===================================");
    ranges = getXRangeFromLong("0,1,2,3,4,5,10,20,50,80,100", SliceType.NUMERIC);
    System.out.println(ranges);

    System.out.println("===================================");
    ranges = getXRangeFromLong("0,5,10,50", SliceType.NUMERIC);
    System.out.println(ranges);

    // pattern =
    // "2012-11-01,2012-11-15,2012-11-20,2012-12-01,2012-12-10,2012-12-20,2013-01-01";
    // ranges = getXRangeFromDate(pattern, DATED);
    // System.out.println(ranges);

  }

}
