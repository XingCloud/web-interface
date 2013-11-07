package com.xingcloud.webinterface.exec;

import static com.xingcloud.webinterface.utils.WebInterfaceDateUtils.canMerge;
import static com.xingcloud.webinterface.utils.WebInterfaceDateUtils.merge;

import com.xingcloud.webinterface.enums.CommonQueryType;
import com.xingcloud.webinterface.enums.Interval;
import com.xingcloud.webinterface.model.EventAndFilterBEPair;
import com.xingcloud.webinterface.model.Filter;
import com.xingcloud.webinterface.model.formula.CommonFormulaQueryDescriptor;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import com.xingcloud.webinterface.utils.WebInterfaceConstants;
import org.apache.commons.collections.CollectionUtils;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class FormulaQueryDescriptorMerger {

  public static Map<EventAndFilterBEPair, Collection<FormulaQueryDescriptor>> mergeDescriptor(
    Collection<FormulaQueryDescriptor> descriptors) throws ParseException {
    if (CollectionUtils.isEmpty(descriptors)) {
      return null;
    }
    Map<EventAndFilterBEPair, Collection<FormulaQueryDescriptor>> map = new HashMap<EventAndFilterBEPair, Collection<FormulaQueryDescriptor>>();
    EventAndFilterBEPair tmpPair, pairInMap;
    Iterator<Entry<EventAndFilterBEPair, Collection<FormulaQueryDescriptor>>> it;
    Collection<FormulaQueryDescriptor> existDescriptors = new ArrayList<FormulaQueryDescriptor>();
    Collection<FormulaQueryDescriptor> newDescriptors;

    Entry<EventAndFilterBEPair, Collection<FormulaQueryDescriptor>> entry;

    boolean merged;
    for (FormulaQueryDescriptor fqd : descriptors) {
      tmpPair = new EventAndFilterBEPair(fqd.getRealBeginDate(), fqd.getRealEndDate(), fqd.getEvent(), fqd.getFilter());
      merged = false;
      it = map.entrySet().iterator();
      existDescriptors.clear();
      while (it.hasNext()) {
        entry = it.next();
        pairInMap = entry.getKey();
        if (canMerge(tmpPair, pairInMap)) {
          existDescriptors.addAll(entry.getValue());
          tmpPair = merge(tmpPair, pairInMap);
          it.remove();
          merged = true;
        }
      }

      if (merged) {
        newDescriptors = new ArrayList<FormulaQueryDescriptor>();
        newDescriptors.addAll(existDescriptors);
      } else {
        newDescriptors = new ArrayList<FormulaQueryDescriptor>(1);
      }
      newDescriptors.add(fqd);
      map.put(tmpPair, newDescriptors);
    }
    return map;
  }

  public static void testSingle() throws ParseException {
    Collection<FormulaQueryDescriptor> descriptors = new ArrayList<FormulaQueryDescriptor>();
    descriptors.add(
      new CommonFormulaQueryDescriptor("a", "2013-01-01", "2013-01-01", "visit.*", WebInterfaceConstants.TOTAL_USER,
                                       WebInterfaceConstants.TOTAL_USER, Filter.ALL, Interval.DAY,
                                       CommonQueryType.NORMAL));
    Map<EventAndFilterBEPair, Collection<FormulaQueryDescriptor>> map = mergeDescriptor(descriptors);

    System.out.println("---------------------------------");
    for (Entry<EventAndFilterBEPair, Collection<FormulaQueryDescriptor>> entry : map.entrySet()) {
      System.out.println(entry.getKey());
      for (FormulaQueryDescriptor fqd : entry.getValue()) {
        System.out.println("\t" + fqd.getRealBeginDate() + " - " + fqd.getRealEndDate());
      }
    }
  }

  public static void testSeparated() throws ParseException {
    List<FormulaQueryDescriptor> descriptors = new ArrayList<FormulaQueryDescriptor>();
    descriptors.add(
      new CommonFormulaQueryDescriptor("a", "2013-01-10", "2013-01-10", "visit.*", WebInterfaceConstants.TOTAL_USER,
                                       WebInterfaceConstants.TOTAL_USER, Filter.ALL, Interval.DAY,
                                       CommonQueryType.NORMAL));
    descriptors.add(
      new CommonFormulaQueryDescriptor("a", "2013-01-12", "2013-01-13", "visit.*", WebInterfaceConstants.TOTAL_USER,
                                       WebInterfaceConstants.TOTAL_USER, Filter.ALL, Interval.DAY,
                                       CommonQueryType.NORMAL));
    descriptors.add(
      new CommonFormulaQueryDescriptor("a", "2013-01-16", "2013-01-20", "visit.*", WebInterfaceConstants.TOTAL_USER,
                                       WebInterfaceConstants.TOTAL_USER, Filter.ALL, Interval.DAY,
                                       CommonQueryType.NORMAL));
    Collections.shuffle(descriptors);

    Map<EventAndFilterBEPair, Collection<FormulaQueryDescriptor>> map = mergeDescriptor(descriptors);

    System.out.println("---------------------------------");
    for (Entry<EventAndFilterBEPair, Collection<FormulaQueryDescriptor>> entry : map.entrySet()) {
      System.out.println(entry.getKey());
      for (FormulaQueryDescriptor fqd : entry.getValue()) {
        System.out.println("\t" + fqd.getRealBeginDate() + " - " + fqd.getRealEndDate());
      }
    }
  }

  public static void testJoined() throws ParseException {
    List<FormulaQueryDescriptor> descriptors = new ArrayList<FormulaQueryDescriptor>();
    descriptors.add(
      new CommonFormulaQueryDescriptor("a", "2013-01-10", "2013-01-10", "visit.*", WebInterfaceConstants.TOTAL_USER,
                                       WebInterfaceConstants.TOTAL_USER, Filter.ALL, Interval.DAY,
                                       CommonQueryType.NORMAL));
    descriptors.add(
      new CommonFormulaQueryDescriptor("a", "2013-01-11", "2013-01-11", "visit.*", WebInterfaceConstants.TOTAL_USER,
                                       WebInterfaceConstants.TOTAL_USER, Filter.ALL, Interval.DAY,
                                       CommonQueryType.NORMAL));
    descriptors.add(
      new CommonFormulaQueryDescriptor("a", "2013-01-12", "2013-01-12", "visit.*", WebInterfaceConstants.TOTAL_USER,
                                       WebInterfaceConstants.TOTAL_USER, Filter.ALL, Interval.DAY,
                                       CommonQueryType.NORMAL));
    Collections.shuffle(descriptors);
    Map<EventAndFilterBEPair, Collection<FormulaQueryDescriptor>> map = mergeDescriptor(descriptors);

    System.out.println("---------------------------------");
    for (Entry<EventAndFilterBEPair, Collection<FormulaQueryDescriptor>> entry : map.entrySet()) {
      System.out.println(entry.getKey());
      for (FormulaQueryDescriptor fqd : entry.getValue()) {
        System.out.println("\t" + fqd.getRealBeginDate() + " - " + fqd.getRealEndDate());
      }
    }
  }

  public static void testComplex() throws ParseException {
    List<FormulaQueryDescriptor> descriptors = new ArrayList<FormulaQueryDescriptor>();
    descriptors.add(
      new CommonFormulaQueryDescriptor("a", "2013-01-01", "2013-01-07", "visit.*", WebInterfaceConstants.TOTAL_USER,
                                       WebInterfaceConstants.TOTAL_USER, Filter.ALL, Interval.DAY,
                                       CommonQueryType.NORMAL));
    descriptors.add(
      new CommonFormulaQueryDescriptor("a", "2013-01-02", "2013-01-03", "visit.*", WebInterfaceConstants.TOTAL_USER,
                                       WebInterfaceConstants.TOTAL_USER, Filter.ALL, Interval.DAY,
                                       CommonQueryType.NORMAL));
    descriptors.add(
      new CommonFormulaQueryDescriptor("a", "2013-01-05", "2013-01-10", "visit.*", WebInterfaceConstants.TOTAL_USER,
                                       WebInterfaceConstants.TOTAL_USER, Filter.ALL, Interval.DAY,
                                       CommonQueryType.NORMAL));
    descriptors.add(
      new CommonFormulaQueryDescriptor("a", "2013-01-12", "2013-01-12", "visit.*", WebInterfaceConstants.TOTAL_USER,
                                       WebInterfaceConstants.TOTAL_USER, Filter.ALL, Interval.DAY,
                                       CommonQueryType.NORMAL));
    descriptors.add(
      new CommonFormulaQueryDescriptor("a", "2013-01-13", "2013-01-16", "visit.*", WebInterfaceConstants.TOTAL_USER,
                                       WebInterfaceConstants.TOTAL_USER, Filter.ALL, Interval.DAY,
                                       CommonQueryType.NORMAL));
    descriptors.add(
      new CommonFormulaQueryDescriptor("a", "2013-01-14", "2013-01-20", "c.*", WebInterfaceConstants.TOTAL_USER,
                                       WebInterfaceConstants.TOTAL_USER, Filter.ALL, Interval.DAY,
                                       CommonQueryType.NORMAL));
    Collections.shuffle(descriptors);

    Map<EventAndFilterBEPair, Collection<FormulaQueryDescriptor>> map = mergeDescriptor(descriptors);

    System.out.println("---------------------------------");
    for (Entry<EventAndFilterBEPair, Collection<FormulaQueryDescriptor>> entry : map.entrySet()) {
      System.out.println(entry.getKey());
      for (FormulaQueryDescriptor fqd : entry.getValue()) {
        System.out.println("\t" + fqd.getRealBeginDate() + " - " + fqd.getRealEndDate());
      }
    }
  }

  public static void main(String[] args) throws ParseException {
    testSingle();
    testSeparated();
    testJoined();
    testComplex();
  }

}
