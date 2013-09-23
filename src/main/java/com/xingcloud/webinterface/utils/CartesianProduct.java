package com.xingcloud.webinterface.utils;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.xingcloud.webinterface.model.Pair;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class CartesianProduct {
  private static final Logger LOGGER = Logger.getLogger(CartesianProduct.class);

  public static void main(String[] args) throws Exception {

    List<Object> aa = new ArrayList<Object>();
    aa.add(1);
    aa.add(2);

    List<Object> bb = new ArrayList<Object>();
    bb.add("a");
    bb.add("b");
    bb.add("c");

    List<Object> cc = new ArrayList<Object>();
    cc.add(DAYS);
    cc.add(HOURS);
    cc.add(MINUTES);
    cc.add(SECONDS);

    Map<String, List<Object>> data = new HashMap<String, List<Object>>();
    data.put("integer", aa);
    data.put("string", bb);
    data.put("timeunit", cc);

  }

  public static void defaultTravelMethod(Pair... pairs) {
    if (ArrayUtils.isEmpty(pairs)) {
      return;
    }
    for (Pair p : pairs) {
      LOGGER.info("[CARTESIAN-PRODUCT] - Only print pair - " + p);
    }
  }

  public static List<Map<String, Object>> makeCartesianProduct(Map<String, String[]> elementMap) {
    List<Map<String, Object>> product = null;
    if (MapUtils.isEmpty(elementMap)) {
      return null;
    }

    String name;
    String[] valueList;

    int elementSize = elementMap.size();
    Map<Integer, String[]> indexedData = new HashMap<Integer, String[]>(elementSize);
    Map<Integer, String> indexedNameData = new HashMap<Integer, String>(elementSize);
    int c = 0, all = 0;

    for (Entry<String, String[]> entry : elementMap.entrySet()) {
      name = entry.getKey();
      valueList = entry.getValue();
      if (ArrayUtils.isEmpty(valueList)) {
        continue;
      }
      all = (all == 0 ? valueList.length : all * valueList.length);
      indexedData.put(c, valueList);
      indexedNameData.put(c, name);
      c++;
    }
    int index = elementSize - 1;
    int[] counter = new int[elementSize];

    Map<String, Object> singleMap;
    for (int i = 0; i < all; i++) {
      singleMap = new HashMap<String, Object>(counter.length);
      for (int j = 0; j < counter.length; j++) {
        name = indexedNameData.get(j);
        valueList = indexedData.get(j);
        singleMap.put(name, valueList[counter[j]]);
      }
      if (product == null) {
        product = new ArrayList<Map<String, Object>>(all);
      }
      product.add(singleMap);
      handleArray(counter, indexedData, index);
    }
    return product;
  }

  public static Object[] travel(Map<String, List<Object>> data, Method m, Object... otherParameters) throws Exception {
    if (m == null) {
      throw new Exception("Cannot invoke null method.");
    }
    int all = 0;

    String key = null;
    List<Object> objectList = null;

    Map<Integer, List<Object>> indexedData = new HashMap<Integer, List<Object>>(data.size());
    Map<Integer, String> indexedNameData = new HashMap<Integer, String>(data.size());
    int c = 0;

    for (Entry<String, List<Object>> entry : data.entrySet()) {
      key = entry.getKey();
      objectList = entry.getValue();
      if (CollectionUtils.isEmpty(objectList)) {
        continue;
      }
      all = (all == 0 ? objectList.size() : all * objectList.size());
      indexedData.put(c, objectList);
      indexedNameData.put(c, key);
      c++;
    }
    int index = data.size() - 1;
    int[] counter = new int[data.size()];

    Pair[] pairs = null;
    Pair pair = null;
    Object[] returnResult = new Object[all];
    Object singleReturnResult = null;
    for (int i = 0; i < all; i++) {
      pairs = new Pair[counter.length];
      for (int j = 0; j < counter.length; j++) {
        pair = new Pair(indexedNameData.get(j), indexedData.get(j).get(counter[j]));
        pairs[j] = pair;
      }
      if (ArrayUtils.isEmpty(otherParameters) || otherParameters.length < 1) {
        defaultTravelMethod(pairs);
      } else {
        singleReturnResult = m.invoke(null, pairs, otherParameters[0]);
      }

      returnResult[i] = singleReturnResult;
      handle(counter, indexedData, index);
    }
    return returnResult;
  }

  public static void handle(int[] counter, Map<Integer, List<Object>> data, int index) {
    counter[index]++;
    if (counter[index] >= data.get(index).size()) {
      counter[index] = 0;
      index--;
      if (index >= 0) {
        handle(counter, data, index);
      }
      index = data.size() - 1;
    }
  }

  public static void handleArray(int[] counter, Map<Integer, String[]> data, int index) {
    counter[index]++;
    if (counter[index] >= data.get(index).length) {
      counter[index] = 0;
      index--;
      if (index >= 0) {
        handleArray(counter, data, index);
      }
      index = data.size() - 1;
    }
  }
}