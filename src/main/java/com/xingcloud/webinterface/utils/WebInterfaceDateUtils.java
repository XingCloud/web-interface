package com.xingcloud.webinterface.utils;

import static com.xingcloud.basic.Constants.DEFAULT_TIME_ZONE;
import static com.xingcloud.basic.utils.DateUtils.date2Short;
import static com.xingcloud.basic.utils.DateUtils.dayDistance;
import static com.xingcloud.basic.utils.DateUtils.getTrueDate;
import static com.xingcloud.basic.utils.DateUtils.isAdjacent;
import static com.xingcloud.basic.utils.DateUtils.long2Date;
import static com.xingcloud.basic.utils.DateUtils.short2Date;
import static com.xingcloud.basic.utils.DateUtils.today;
import static com.xingcloud.webinterface.enums.BetweenMode.CLOSE_CLOSE;

import com.google.common.base.Strings;
import com.xingcloud.webinterface.enums.BetweenMode;
import com.xingcloud.webinterface.model.EventAndFilterBEPair;
import com.xingcloud.webinterface.model.Filter;

import java.text.ParseException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;

public class WebInterfaceDateUtils {

  /**
   * 只判断重叠
   *
   * @param thisBeginDate
   * @param thisEndDate
   * @param anotherBeginDate
   * @param anotherEndDate
   * @return
   * @throws java.text.ParseException
   */
  public static boolean isOverlap(Date thisBeginDate, Date thisEndDate, Date anotherBeginDate,
                                  Date anotherEndDate) throws ParseException {
    if (between(thisBeginDate, thisEndDate, anotherBeginDate, CLOSE_CLOSE) && between(thisBeginDate, thisEndDate,
                                                                                      anotherEndDate, CLOSE_CLOSE)) {
      return true;
    }
    // 另一个pair的日期包含本pair的所有
    else if (between(anotherBeginDate, anotherEndDate, thisBeginDate, CLOSE_CLOSE) && between(anotherBeginDate,
                                                                                              anotherEndDate,
                                                                                              thisEndDate,
                                                                                              CLOSE_CLOSE)) {
      return true;
    }
    // 本pair包含另一个pair的开始日期
    else if (between(thisBeginDate, thisEndDate, anotherBeginDate, CLOSE_CLOSE) && !between(thisBeginDate, thisEndDate,
                                                                                            anotherEndDate,
                                                                                            CLOSE_CLOSE)) {
      return true;
    }
    // 本pair包含另一个pair的结束日期
    else if (!between(thisBeginDate, thisEndDate, anotherBeginDate, CLOSE_CLOSE) && between(thisBeginDate, thisEndDate,
                                                                                            anotherEndDate,
                                                                                            CLOSE_CLOSE)) {
      return true;
    }
    return false;
  }

  public static boolean isAdjacentOrOverlap(Date thisBeginDate, Date thisEndDate, Date anotherBeginDate,
                                            Date anotherEndDate) throws ParseException {
    return isAdjacent(thisEndDate, anotherBeginDate) || isAdjacent(thisBeginDate, anotherEndDate) || isOverlap(
      thisBeginDate, thisEndDate, anotherBeginDate, anotherEndDate);
  }

  /**
   * 两个Pair中的日期是否有重叠, 或首尾相接, 并且事件可以合并
   *
   * @param p1
   * @param p2
   * @throws java.text.ParseException
   */
  public static boolean canMerge(EventAndFilterBEPair p1, EventAndFilterBEPair p2) throws ParseException {
    String k1 = p1.getKey();
    String k2 = p2.getKey();
    Filter f1 = p1.getFilter();
    Filter f2 = p2.getFilter();

    if (k1 == null || k2 == null) {
      return false;
    }
    if (!k1.equals(k2)) {
      return false;
    }

    if (f1 == null || f2 == null) {
      return false;
    }
    if (!f1.equals(f2)) {
      return false;
    }

    Date thisBeginDate = short2Date(p1.getBeginDate());
    Date thisEndDate = short2Date(p1.getEndDate());
    Date anotherBeginDate = short2Date(p2.getBeginDate());
    Date anotherEndDate = short2Date(p2.getEndDate());
    // // 本pair包含另一个pair的日期的所有
    // boolean result = false;
    // if (between(thisBeginDate, thisEndDate, anotherBeginDate,
    // CLOSE_CLOSE)
    // && between(thisBeginDate, thisEndDate, anotherEndDate,
    // CLOSE_CLOSE)) {
    // result = true;
    // }
    // // 另一个pair的日期包含本pair的所有
    // else if (between(anotherBeginDate, anotherEndDate, thisBeginDate,
    // CLOSE_CLOSE)
    // && between(anotherBeginDate, anotherEndDate, thisEndDate,
    // CLOSE_CLOSE)) {
    // result = true;
    // }
    // // 本pair包含另一个pair的开始日期
    // else if (between(thisBeginDate, thisEndDate, anotherBeginDate,
    // CLOSE_CLOSE)
    // && !between(thisBeginDate, thisEndDate, anotherEndDate,
    // CLOSE_CLOSE)) {
    // result = true;
    // }
    // // 本pair包含另一个pair的结束日期
    // else if (!between(thisBeginDate, thisEndDate, anotherBeginDate,
    // CLOSE_CLOSE)
    // && between(thisBeginDate, thisEndDate, anotherEndDate,
    // CLOSE_CLOSE)) {
    // result = true;
    // }
    // // 本pair的结尾可以和另一个pair的起始相拼接
    // else if (isAdjacent(thisEndDate, anotherBeginDate)) {
    // result = true;
    // }
    // // 本pair的开始可以和另一个pair的结尾相拼接
    // else if (isAdjacent(thisBeginDate, anotherEndDate)) {
    // result = true;
    // } else {
    // result = false;
    // }
    return isAdjacentOrOverlap(thisBeginDate, thisEndDate, anotherBeginDate, anotherEndDate);
  }

  /**
   * 合并2个pair中的起始和结束日期, 以最小的日期作为新的beginDate, 最大的日期作为新的endDate
   *
   * @param p1
   * @param p2
   * @return
   * @throws java.text.ParseException
   */
  public static EventAndFilterBEPair merge(EventAndFilterBEPair p1, EventAndFilterBEPair p2) throws ParseException {
    Date[] dates = new Date[4];
    dates[0] = short2Date(p1.getBeginDate());
    dates[1] = short2Date(p1.getEndDate());
    dates[2] = short2Date(p2.getBeginDate());
    dates[3] = short2Date(p2.getEndDate());
    Arrays.sort(dates);
    return new EventAndFilterBEPair(date2Short(dates[0]), date2Short(dates[3]), p1.getKey(), p1.getFilter());
  }

  // /**
  // * 将一组独立的Pair合并成首尾不相接, 不重合的pair
  // *
  // * @param pairs
  // * @return
  // * @throws ParseException
  // */
  // public static Set<EventAndFilterBEPair> merge(
  // Set<EventAndFilterBEPair> pairs ) throws ParseException {
  // Set<EventAndFilterBEPair> merged = new HashSet<EventAndFilterBEPair>();
  // Iterator<EventAndFilterBEPair> it = null;
  //
  // String k = "a";
  // EventAndFilterBEPair mergerdPair = null;
  // EventAndFilterBEPair tmpPair = null;
  // boolean b = false;
  // for( EventAndFilterBEPair pair: pairs ) {
  // it = merged.iterator();
  //
  // tmpPair = pair;
  // while (it.hasNext()) {
  // mergerdPair = it.next();
  // if (canMerge(tmpPair, mergerdPair, k, k)) {
  // b = true;
  // tmpPair = merge(tmpPair, mergerdPair, k);
  // it.remove();
  // }
  // }
  //
  // if (b) {
  // merged.add(tmpPair);
  // } else {
  // merged.add(pair);
  // }
  // }
  //
  // return merged;
  // }

  /**
   * 1个日期是否在两个日期之间(左闭右闭)
   *
   * @param begenDate
   * @param endDate
   * @param targetDate
   * @param mode
   * @return
   * @throws java.text.ParseException
   */
  public static boolean between(String begenDate, String endDate, String targetDate, BetweenMode mode) throws
    ParseException {
    if (Strings.isNullOrEmpty(begenDate) || Strings.isNullOrEmpty(endDate) || Strings.isNullOrEmpty(targetDate)) {
      return false;
    }
    Date bDate = short2Date(begenDate);
    Date eDate = short2Date(endDate);
    Date tDate = long2Date(targetDate);
    return between(bDate, eDate, tDate, mode);
  }

  /**
   * 1个日期是否在两个日期之间(带有开闭区间模式)
   *
   * @param begenDate
   * @param endDate
   * @param targetDate
   * @param mode
   * @return
   * @throws java.text.ParseException
   */
  public static boolean between(Date begenDate, Date endDate, Date targetDate, BetweenMode mode) throws ParseException {
    if (begenDate == null || endDate == null || targetDate == null) {
      return false;
    }
    Calendar c = Calendar.getInstance(DEFAULT_TIME_ZONE);
    switch (mode) {
      case OPEN_OPEN:
        c.setTime(begenDate);
        c.add(Calendar.DATE, 1);
        begenDate = c.getTime();
        c.setTime(endDate);
        c.add(Calendar.DATE, -1);
        endDate = c.getTime();
        break;
      case OPEN_CLOSE:
        c.setTime(begenDate);
        c.add(Calendar.DATE, 1);
        begenDate = c.getTime();
        break;
      case CLOSE_OPEN:
        c.setTime(endDate);
        c.add(Calendar.DATE, -1);
        endDate = c.getTime();
        break;
      default:
        break;
    }
    if (targetDate.before(begenDate) || targetDate.after(endDate)) {
      return false;
    }
    return true;
  }

  public static void main(String[] args) throws ParseException {
    Date currentDate = today();
    System.out.println(currentDate);
    String beginDate = "2012-07-04";
    String endDate = "2012-07-08";
    String targetDate1 = "2012-07-09";
    String targetDate2 = "2012-07-10";

    System.out.println(
      between(short2Date(beginDate), short2Date(endDate), short2Date(targetDate1), BetweenMode.CLOSE_CLOSE));
    System.out.println(
      between(short2Date(beginDate), short2Date(endDate), short2Date(targetDate1), BetweenMode.CLOSE_OPEN));
    System.out.println(
      between(short2Date(beginDate), short2Date(endDate), short2Date(targetDate1), BetweenMode.OPEN_CLOSE));
    System.out
          .println(between(short2Date(beginDate), short2Date(endDate), short2Date(targetDate1), BetweenMode.OPEN_OPEN));

    System.out.println(
      between(short2Date(beginDate), short2Date(endDate), short2Date(targetDate2), BetweenMode.CLOSE_CLOSE));
    System.out.println(
      between(short2Date(beginDate), short2Date(endDate), short2Date(targetDate2), BetweenMode.CLOSE_OPEN));
    System.out.println(
      between(short2Date(beginDate), short2Date(endDate), short2Date(targetDate2), BetweenMode.OPEN_CLOSE));
    System.out
          .println(between(short2Date(beginDate), short2Date(endDate), short2Date(targetDate2), BetweenMode.OPEN_OPEN));

    System.out.println("===============");
    System.out.println(isAdjacent("2012-07-18", "2012-07-18"));
    System.out.println(isAdjacent("2012-07-18", "2012-07-19"));
    System.out.println(isAdjacent("2012-07-18", "2012-07-20"));

    System.out.println(isAdjacent("2012-07-18", "2012-07-18"));
    System.out.println(isAdjacent("2012-07-18", "2012-07-17"));
    System.out.println(isAdjacent("2012-07-18", "2012-07-16"));

    System.out.println(Arrays.toString(getTrueDate("2012-08-19", -4, -4, new Date())));
    System.out.println("===================================");
    System.out.println(dayDistance("2012-09-26", "2012-09-25"));
  }

  public static long short2UserTableFormat(String date) {
    if (date == null) {
      return -1;
    }
    String s = date.replace("-", "");
    Long l = Long.valueOf(s);
    return l * 1000000;
  }

  public static String short2EventTableFormat(String date) {
    return date == null ? null : date.replace("-", "");
  }

}
