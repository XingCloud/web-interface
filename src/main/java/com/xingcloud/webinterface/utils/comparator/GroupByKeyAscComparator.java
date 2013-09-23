package com.xingcloud.webinterface.utils.comparator;

import static com.xingcloud.webinterface.utils.WebInterfaceCommonUtils.compareObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class GroupByKeyAscComparator implements Comparator<Object[]> {

  public int compare(Object[] o1, Object[] o2) {
    if (o1 == null || o2 == null || o1.length < 1 || o2.length < 1 || o1[0] == null || o2[0] == null) {
      return 0;
    }
    Object oo1 = o1[0];
    Object oo2 = o2[0];

    int v = compareObject(oo1, oo2);
    if (v != 0) {
      return v;
    }
    return compareObject(o1[1], o2[1]);
  }

  public static void main(String[] args) {
    Object[] o1 = new Object[]{9, 3};
    Object[] o2 = new Object[]{"a", 1};
    Object[] o3 = new Object[]{"2", 5};
    List<Object[]> list = new ArrayList<Object[]>();
    list.add(o2);
    list.add(o1);
    list.add(o3);
    Collections.sort(list, new GroupByKeyAscComparator());

    for (Object[] o : list) {
      System.out.println(Arrays.toString(o));
    }

  }
}
