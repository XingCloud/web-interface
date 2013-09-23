package com.xingcloud.webinterface.utils.comparator;

public class GroupByKeyDescComparator extends GroupByKeyAscComparator {

  @Override
  public int compare(Object[] o1, Object[] o2) {
    return -super.compare(o1, o2);
  }

}
