package com.xingcloud.webinterface.model;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class EventAndFilterBEPair extends BeginEndDatePair {
  private static final long serialVersionUID = -3371201730040048967L;
  private String key;

  private Filter filter;

  public EventAndFilterBEPair() {
    super();
  }

  public EventAndFilterBEPair(String beginDate, String endDate, String key, Filter filter) {
    super(beginDate, endDate);
    this.key = key;
    this.filter = filter;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public Filter getFilter() {
    return filter;
  }

  public void setFilter(Filter filter) {
    this.filter = filter;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((filter == null) ? 0 : filter.hashCode());
    result = prime * result + ((key == null) ? 0 : key.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    EventAndFilterBEPair other = (EventAndFilterBEPair) obj;
    if (filter == null) {
      if (other.filter != null)
        return false;
    } else if (!filter.equals(other.filter))
      return false;
    if (key == null) {
      if (other.key != null)
        return false;
    } else if (!key.equals(other.key))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "PWK.(" + getBeginDate() + "." + getEndDate() + ")#" + this.key + "#" + this.filter;
  }

  public static void main(String[] args) throws ParseException {
    DecimalFormat df = new DecimalFormat("00");
    List<EventAndFilterBEPair> pairs = new ArrayList<EventAndFilterBEPair>();
    for (int i = 1; i < 21; i++) {
      pairs.add(new EventAndFilterBEPair("2012-07-" + df.format(i), "2012-07-" + df.format(i), "a", Filter.ALL));
    }
    Iterator<EventAndFilterBEPair> it = pairs.iterator();
    Random r = new Random();
    while (it.hasNext()) {
      it.next();
      if (r.nextBoolean()) {
        it.remove();
      }
    }
    for (EventAndFilterBEPair p : pairs) {
      System.out.println(p);
    }

    // for( int i = 0; i < 10; i++ ) {
    // System.out.println("===============");
    // Collections.shuffle(pairs);
    // for( EventAndFilterBEPair p: merge(new HashSet<EventAndFilterBEPair>(
    // pairs)) ) {
    // System.out.println(p);
    // }
    // }

    // BeginEndDatePair pair1 = null;
    // BeginEndDatePair pair2 = null;
    // String k1 = "a";
    // String k2 = "a";
    // // p1等于p2
    // pair1 = new BeginEndDatePair("2012-06-10", "2012-07-10");
    // pair2 = new BeginEndDatePair("2012-06-10", "2012-07-10");
    // System.out.println(isOverlapOrAdjacent(pair1, pair2, k1, k2));
    //
    // // p2小
    // pair1 = new BeginEndDatePair("2012-06-10", "2012-07-10");
    // pair2 = new BeginEndDatePair("2012-06-10", "2012-07-09");
    // System.out.println(isOverlapOrAdjacent(pair1, pair2, k1, k2));
    //
    // pair1 = new BeginEndDatePair("2012-06-10", "2012-07-10");
    // pair2 = new BeginEndDatePair("2012-06-09", "2012-07-10");
    // System.out.println(isOverlapOrAdjacent(pair1, pair2, k1, k2));
    //
    // pair1 = new BeginEndDatePair("2012-06-10", "2012-07-10");
    // pair2 = new BeginEndDatePair("2012-06-09", "2012-07-09");
    // System.out.println(isOverlapOrAdjacent(pair1, pair2, k1, k2));
    //
    // // p2大
    // pair1 = new BeginEndDatePair("2012-06-10", "2012-07-10");
    // pair2 = new BeginEndDatePair("2012-06-09", "2012-07-10");
    // System.out.println(isOverlapOrAdjacent(pair1, pair2, k1, k2));
    //
    // pair1 = new BeginEndDatePair("2012-06-10", "2012-07-10");
    // pair2 = new BeginEndDatePair("2012-06-10", "2012-07-11");
    // System.out.println(isOverlapOrAdjacent(pair1, pair2, k1, k2));
    //
    // pair1 = new BeginEndDatePair("2012-06-10", "2012-07-10");
    // pair2 = new BeginEndDatePair("2012-06-09", "2012-07-11");
    // System.out.println(isOverlapOrAdjacent(pair1, pair2, k1, k2));
    //
    // // 部分重叠
    // pair1 = new BeginEndDatePair("2012-06-10", "2012-07-10");
    // pair2 = new BeginEndDatePair("2012-06-01", "2012-07-01");
    // System.out.println(isOverlapOrAdjacent(pair1, pair2, k1, k2));
    //
    // pair1 = new BeginEndDatePair("2012-06-10", "2012-07-10");
    // pair2 = new BeginEndDatePair("2012-07-01", "2012-07-11");
    // System.out.println(isOverlapOrAdjacent(pair1, pair2, k1, k2));
    //
    // // 无交集
    // pair1 = new BeginEndDatePair("2012-06-10", "2012-07-10");
    // pair2 = new BeginEndDatePair("2012-06-01", "2012-06-09");
    // System.out.println(isOverlapOrAdjacent(pair1, pair2, k1, k2));
    //
    // pair1 = new BeginEndDatePair("2012-06-10", "2012-07-10");
    // pair2 = new BeginEndDatePair("2012-07-11", "2012-07-19");
    // System.out.println(isOverlapOrAdjacent(pair1, pair2, k1, k2));
    //
    // BeginEndDatePair p = new BeginEndDatePairWithKey("2012-07-18",
    // "2012-07-19", "asdf");
    // System.out.println(p.toString());

  }
}
