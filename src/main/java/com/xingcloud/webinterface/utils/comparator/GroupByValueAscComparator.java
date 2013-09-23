package com.xingcloud.webinterface.utils.comparator;

import static com.xingcloud.webinterface.utils.WebInterfaceCommonUtils.compareObject;

import java.util.Comparator;
import java.util.Map.Entry;

public class GroupByValueAscComparator implements Comparator<Entry<Object, Number>> {

  public int compare(Entry<Object, Number> e1, Entry<Object, Number> e2) {
    if (e1 == null && e2 == null) {
      return 0;
    } else if (e1 == null && e2 != null) {
      return -1;
    } else if (e1 != null && e2 == null) {
      return 1;
    }
    Number n1 = e1.getValue();
    Number n2 = e2.getValue();
    if (n1 == null && n2 == null) {
      return compareObject(e1.getKey(), e2.getKey());
    } else if (n1 == null && n2 != null) {
      return -1;
    } else if (n1 != null && n2 == null) {
      return 1;
    }

    double x = e1.getValue().doubleValue();
    double y = e2.getValue().doubleValue();
    if (x > y) {
      return 1;
    } else if (x < y) {
      return -1;
    } else {
      return 0;
    }
  }
}
