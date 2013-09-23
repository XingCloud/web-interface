package com.xingcloud.webinterface.model.aggregation;

import static com.xingcloud.webinterface.model.ResultTuple.NA_RESULT_TUPLE;
import static com.xingcloud.webinterface.model.ResultTuple.PENDING_RESULT_TUPLE;
import static com.xingcloud.webinterface.model.ResultTuple.doAccumulation;
import static com.xingcloud.webinterface.utils.IntermediateResultUtils.spreadStatus;

import com.xingcloud.webinterface.exception.DataFillingException;
import com.xingcloud.webinterface.model.Pending;
import com.xingcloud.webinterface.model.ResultTuple;
import com.xingcloud.webinterface.model.intermediate.KeyTuple;

import java.util.HashMap;
import java.util.Map;

public class Accumulator {

  private static ResultTuple accumulateGenericly(ResultTuple rt1, ResultTuple rt2) {
    if (rt1 == null && rt2 == null) {
      return null;
    }
    if (rt1 == null && rt2 != null) {
      return rt2.duplicate();
    }
    if (rt1 != null && rt2 == null) {
      return rt1.duplicate();
    }

    return null;
  }

  private static KeyTuple accumulateGenericly(KeyTuple kt1, KeyTuple kt2) throws DataFillingException {

    if (kt1 == null && kt2 != null) {
      return new KeyTuple(kt2.getKey(), kt2.getAdditionalKey(), kt2.getResultTuple().duplicate(), kt2.getStatus());
    }

    if (kt1 != null && kt2 == null) {
      return new KeyTuple(kt1.getKey(), kt1.getAdditionalKey(), kt1.getResultTuple().duplicate(), kt1.getStatus());
    }

    if (!kt1.getKey().equals(kt2.getKey())) {
      throw new DataFillingException("Cannot merge " + kt1 + " and " + kt2 + " because they have different key.");
    }
    return null;
  }

  public static ResultTuple accumulateStrictly(ResultTuple rt1, ResultTuple rt2) {
    if (rt1 == null && rt2 == null) {
      return null;
    }
    ResultTuple rt = accumulateGenericly(rt1, rt2);
    if (rt != null) {
      return rt;
    }

    return null;
  }

  /**
   * 严格累加, 即在结果中出现Pending或NotAvailable时, 一律忽略该项 即有NA出现, 返回NA, 有PENDING出现, 返回PENDING, 否则累加值KT的value
   * 例:x={"a":1,"b":1},y={"a":PendingInstance,"b":1}, 则累加结果为z={"b":2}
   *
   * @throws DataFillingException
   */
  public static KeyTuple accumulateStrictly(KeyTuple kt1, KeyTuple kt2) throws DataFillingException {
    if (kt1 == null && kt2 == null) {
      return null;
    }

    KeyTuple kt = accumulateGenericly(kt1, kt2);
    if (kt != null) {
      return kt;
    }

    Object genericKey = kt1.getKey();
    Object genericAdditionKey = kt1.getAdditionalKey();
    Object genericStatus = null;

    genericStatus = spreadStatus(kt1.getStatus(), kt2.getStatus());

    ResultTuple kt1RT = kt1.getResultTuple();
    ResultTuple kt2RT = kt2.getResultTuple();

    // 有NA的直接NA
    if (kt1RT.isNAPlaceholder() || kt2RT.isNAPlaceholder()) {
      return new KeyTuple(genericKey, genericAdditionKey, NA_RESULT_TUPLE, genericStatus);
    }

    // 有Pending的直接pending
    if (kt1RT.isPendingPlaceholder() || kt2RT.isPendingPlaceholder()) {
      return new KeyTuple(genericKey, genericAdditionKey, PENDING_RESULT_TUPLE, genericStatus);
    }

    ResultTuple mergedResultTuple = doAccumulation(kt1RT, kt2RT);

    return new KeyTuple(genericKey, genericAdditionKey, mergedResultTuple, genericStatus);
  }

  /**
   * 宽松累加, kt1和kt2中只要有一项有数值, 就直接加出结果, 不论另一项是na还是pending
   *
   * @param kt1
   * @param kt2
   * @return
   * @throws DataFillingException
   */
  public static KeyTuple accumulateLoosely(KeyTuple kt1, KeyTuple kt2) throws DataFillingException {
    if (kt1 == null && kt2 == null) {
      return null;
    }

    KeyTuple kt = accumulateGenericly(kt1, kt2);
    if (kt != null) {
      return kt;
    }

    Object genericKey = kt1.getKey();
    Object genericAdditionKey = kt1.getAdditionalKey();
    Object genericStatus = null;

    if (Pending.INSTANCE.equals(kt1.getStatus()) || Pending.INSTANCE.equals(kt2.getStatus())) {
      genericStatus = Pending.INSTANCE;
    }

    ResultTuple kt1RT = kt1.getResultTuple();
    ResultTuple kt2RT = kt2.getResultTuple();

    if (kt1RT.isValidResultTuple() && kt2RT.isValidResultTuple()) {
      ResultTuple mergedResultTuple = doAccumulation(kt1RT, kt2RT);
      return new KeyTuple(genericKey, genericAdditionKey, mergedResultTuple, genericStatus);
    }

    if (kt1RT.isPendingPlaceholder() || kt2RT.isPendingPlaceholder()) {
      return new KeyTuple(genericKey, genericAdditionKey, PENDING_RESULT_TUPLE, genericStatus);
    }

    return new KeyTuple(genericKey, genericAdditionKey, NA_RESULT_TUPLE, genericStatus);
  }

  public static void main(String[] args) {
    Map<Integer, Integer> m1 = new HashMap<Integer, Integer>();
    Map<Integer, Integer> m2 = new HashMap<Integer, Integer>();

    m1.put(1, 1);

    m2 = new HashMap<Integer, Integer>(m1);
    m2.put(1, 2);
    System.out.println(m1);
    System.out.println(m2);
  }

}
