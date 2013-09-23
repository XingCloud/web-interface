package com.xingcloud.webinterface.utils.comparator;

import java.util.Map.Entry;

public class GroupByValueDescComparator extends GroupByValueAscComparator {

  @Override
  public int compare(Entry<Object, Number> e1, Entry<Object, Number> e2) {
    return -super.compare(e1, e2);
  }

}
