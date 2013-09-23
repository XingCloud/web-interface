package com.xingcloud.webinterface.utils.comparator;

import java.util.Comparator;

public class ObjectArrayAscComparator implements Comparator<Object[]> {

  public int compare(Object[] o1, Object[] o2) {
    if (o1 == null || o2 == null || o1.length < 1 || o2.length < 1) {
      return 0;
    }
    Object oo1 = o1[0];
    Object oo2 = o2[0];
    if (!((oo1 instanceof Comparable) && (oo2 instanceof Comparable))) {
      return 0;
    }
    Comparable c1 = (Comparable) oo1;
    Comparable c2 = (Comparable) oo2;
    return c1.compareTo(c2);
  }

}
