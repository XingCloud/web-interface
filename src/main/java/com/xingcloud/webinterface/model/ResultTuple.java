package com.xingcloud.webinterface.model;

import static com.xingcloud.webinterface.enums.ResultTupleState.VALID;

import com.google.gson.annotations.Expose;
import com.xingcloud.webinterface.enums.Function;
import com.xingcloud.webinterface.enums.ResultTupleState;
import org.apache.commons.lang.ArrayUtils;

import java.io.Serializable;

public class ResultTuple implements Serializable {

  private static final long serialVersionUID = 4338055652233270973L;

  public static final ResultTuple EMPTY_RESULT_TUPLE = new ResultTuple(0, 0, 0);

  public static final ResultTuple NA_RESULT_TUPLE = new ResultTuple(ResultTupleState.NA);

  public static final ResultTuple PENDING_RESULT_TUPLE = new ResultTuple(ResultTupleState.PENDING);

  // Sampling data
  @Expose
  private Number[] tuple = new Number[3];
  private double samplingRate = 1d;
  private ResultTupleState resultTupleState;

  public ResultTuple duplicate() {
    ResultTupleState state = getResultTupleState();
    ResultTuple rt = new ResultTuple(state);
    System.arraycopy(this.tuple, 0, rt.getTuple(), 0, this.tuple.length);
    double samplingRate = getSamplingRate();
    rt.setSamplingRate(samplingRate);
    return rt;
  }

  public ResultTuple(ResultTupleState resultTupleState) {
    super();
    this.resultTupleState = resultTupleState;
  }

  public ResultTuple(Number cnt, Number sum, Number usernum) {
    super();
    this.tuple[0] = cnt;
    this.tuple[1] = sum;
    this.tuple[2] = usernum;
    this.resultTupleState = VALID;
  }

  public ResultTuple(Number cnt, Number sum, Number usernum, double samplingRate) {
    super();
    this.tuple[0] = cnt;
    this.tuple[1] = sum;
    this.tuple[2] = usernum;
    this.samplingRate = samplingRate;
    this.resultTupleState = VALID;
  }

  public static ResultTuple createNewEmptyResultTuple() {
    return new ResultTuple(0, 0, 0);
  }

  public static ResultTuple createNewNullResultTuple() {
    return new ResultTuple(null, null, null);
  }

  public boolean isValidResultTuple() {
    return !(isNAPlaceholder() || isPendingPlaceholder());
  }

  public boolean isNAPlaceholder() {
    return ResultTupleState.NA.equals(this.resultTupleState);
  }

  public boolean isPendingPlaceholder() {
    return ResultTupleState.PENDING.equals(this.resultTupleState);
  }

  public Number getEstimateValue(Function function) {
    switch (function) {
      case SUM:
        return getEstimateSum();
      case COUNT:
        return getEstimateCount();
      case USER_NUM:
        return getEstimateUsernum();
      default:
        return null;
    }
  }

  public void setCount(Number cnt) {
    this.tuple[0] = cnt;
  }

  public void setSum(Number sum) {
    this.tuple[1] = sum;
  }

  public void setUsernum(Number usernum) {
    this.tuple[2] = usernum;
  }

  public Number getCount() {
    return this.tuple[0];
  }

  public Number getSum() {
    return this.tuple[1];
  }

  public Number getUsernum() {
    return this.tuple[2];
  }

  public Number getEstimateCount() {
    if (this.tuple[0] == null) {
      return null;
    }
    return Math.round(this.tuple[0].doubleValue() / samplingRate);
  }

  public Number getEstimateSum() {
    if (this.tuple[1] == null) {
      return null;
    }
    return this.tuple[1].doubleValue() / samplingRate;
  }

  public Number getEstimateUsernum() {
    if (this.tuple[2] == null) {
      return null;
    }
    return Math.round(this.tuple[2].doubleValue() / samplingRate);
  }

  public static ResultTuple doAccumulation(ResultTuple... rts) {
    if (ArrayUtils.isEmpty(rts)) {
      return null;
    }

    Number accumulatedCount = null;
    Number accumulatedSum = null;
    Number accumulatedUserNum = null;

    Number operatedCount, operatedSum, operatedUserNum;

    for (ResultTuple rt : rts) {
      operatedCount = rt.getEstimateCount();
      operatedSum = rt.getEstimateSum();
      operatedUserNum = rt.getEstimateUsernum();

      if (operatedCount != null) {
        accumulatedCount = (accumulatedCount == null ? operatedCount.longValue()
                                                     : (operatedCount.longValue() + accumulatedCount.longValue()
                            )
        );
      }
      if (operatedSum != null) {
        accumulatedSum = (accumulatedSum == null ? operatedSum.longValue()
                                                 : (operatedSum.longValue() + accumulatedSum.longValue()
                          )
        );
      }
      if (operatedUserNum != null) {
        accumulatedUserNum = (accumulatedUserNum == null ? operatedUserNum.longValue()
                                                         : (operatedUserNum.longValue() + accumulatedUserNum.longValue()
                              )
        );
      }

    }

    ResultTuple rt = new ResultTuple(accumulatedCount, accumulatedSum, accumulatedUserNum, 1d);

    return rt;
  }

  public void doDivision(int counter) {
    if (counter == 0) {
      return;
    }
    Number cnt = getEstimateCount();
    setCount(cnt == null ? null : cnt.doubleValue() / counter);
    Number sum = getEstimateSum();
    setSum(sum == null ? null : sum.doubleValue() / counter);
    Number usn = getEstimateUsernum();
    setUsernum(usn == null ? null : usn.doubleValue() / counter);
    setSamplingRate(1d);
  }

  public void incAll(Number count, Number sum, Number usernum) {
    incCount(count);
    incSum(sum);
    incUsernum(usernum);
  }

  public void incAll(ResultTuple rt) {
    if (rt == null) {
      return;
    }
    incCount(rt);
    incSum(rt);
    incUsernum(rt);
  }

  public void incCount(Number count) {
    Number thisValue = getEstimateCount();
    if (thisValue == null && count == null) {
      return;
    }
    setCount((thisValue == null ? 0 : thisValue.longValue()) + (count == null ? 0 : count.longValue()
    ));
    setSamplingRate(1d);
  }

  public void incCount(ResultTuple rt) {
    Number thisValue = getEstimateCount();
    Number thatValue = rt.getEstimateCount();
    if (thisValue == null && thatValue == null) {
      return;
    }

    setCount((thisValue == null ? 0 : thisValue.longValue()) + (thatValue == null ? 0 : thatValue.longValue()
    ));
    setSamplingRate(1d);
  }

  public void incSum(Number sum) {
    Number thisValue = getEstimateSum();
    if (thisValue == null && sum == null) {
      return;
    }
    setSum((thisValue == null ? 0 : thisValue.longValue()) + (sum == null ? 0 : sum.longValue()
    ));
    setSamplingRate(1d);
  }

  public void incSum(ResultTuple rt) {
    Number thisValue = getEstimateSum();
    Number thatValue = rt.getEstimateSum();
    if (thisValue == null && thatValue == null) {
      return;
    }

    setSum((thisValue == null ? 0 : thisValue.longValue()) + (thatValue == null ? 0 : thatValue.longValue()
    ));
    setSamplingRate(1d);
  }

  public void incUsernum(Number usernum) {
    Number thisValue = getEstimateUsernum();
    if (thisValue == null && usernum == null) {
      return;
    }
    setUsernum((thisValue == null ? 0 : thisValue.longValue()) + (usernum == null ? 0 : usernum.longValue()
    ));
    setSamplingRate(1d);
  }

  public void incUsernum(ResultTuple rt) {
    Number thisValue = getEstimateUsernum();
    Number thatValue = rt.getEstimateUsernum();
    if (thisValue == null && thatValue == null) {
      return;
    }
    setUsernum((thisValue == null ? 0 : thisValue.longValue()) + (thatValue == null ? 0 : thatValue.longValue()
    ));
    setSamplingRate(1d);
  }

  public String toString() {
    return "(C=" + tuple[0] + ".U=" + tuple[2] + ".S=" + tuple[1] + "@" + samplingRate + ")";
  }

  public Number[] getTuple() {
    return tuple;
  }

  public double getSamplingRate() {
    return samplingRate;
  }

  public void setSamplingRate(double samplingRate) {
    this.samplingRate = samplingRate;
  }

  public ResultTupleState getResultTupleState() {
    return resultTupleState;
  }

  public void setResultTupleState(ResultTupleState resultTupleState) {
    this.resultTupleState = resultTupleState;
  }

  public void expandOrContract(double rate) {
    if (isNAPlaceholder() || isPendingPlaceholder()) {
      return;
    }
    if (this.tuple == null) {
      return;
    }
    if (this.tuple[0] != null) {
      this.tuple[0] = this.tuple[0].doubleValue() * rate;
    }
    if (this.tuple[1] != null) {
      this.tuple[1] = this.tuple[1].doubleValue() * rate;
    }
    if (this.tuple[2] != null) {
      this.tuple[2] = this.tuple[2].doubleValue() * rate;
    }
  }
}
