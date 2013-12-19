package com.xingcloud.webinterface.calculate;

import com.xingcloud.basic.utils.DateUtils;
import com.xingcloud.webinterface.exception.FormulaException;

import java.text.ParseException;

/**
 * User: Z J Wu Date: 13-12-16 Time: 下午2:49 Package: com.xingcloud.webinterface.calculate.func
 */
public class Scale {
  public static final double DEFAULT_SCALE = 1d;
  public static final long DEFAULT_TIMESTAMP = -28800001;
  private long lowerDate = DEFAULT_TIMESTAMP;
  private String lowerDateString;
  private long upperDate = DEFAULT_TIMESTAMP;
  private String upperDateString;
  private double scaleValue;

  private Scale() {
  }

  public double getScaleValue() {
    return scaleValue;
  }

  public static Scale buildRangeScale(String lowerDate, String upperDate, double scaleValue) throws FormulaException {
    Scale scale = new Scale();
    try {
      scale.lowerDate = DateUtils.short2Date(lowerDate).getTime();
    } catch (ParseException e) {
      throw new FormulaException("Cannot parse lower date of scale(" + scaleValue + ") - " + lowerDate);
    }
    try {
      scale.upperDate = DateUtils.short2Date(upperDate).getTime();
    } catch (ParseException e) {
      throw new FormulaException("Cannot parse upper date of scale(" + scaleValue + ") - " + upperDate);
    }
    scale.lowerDateString = lowerDate;
    scale.upperDateString = upperDate;
    scale.scaleValue = scaleValue;
    return scale;
  }

  public static Scale buildLowerScale(String lowerDate, double scaleValue) throws FormulaException {
    Scale scale = new Scale();
    try {
      scale.lowerDate = DateUtils.short2Date(lowerDate).getTime();
    } catch (ParseException e) {
      throw new FormulaException("Cannot parse lower date of scale(" + scale + ") - " + lowerDate);
    }
    scale.lowerDateString = lowerDate;
    scale.scaleValue = scaleValue;
    return scale;
  }

  public static Scale buildDefaultScale(double scaleValue) throws FormulaException {
    Scale scale = new Scale();
    scale.scaleValue = scaleValue;
    return scale;
  }

  public Double accept(String date) throws FormulaException {
    long d;
    try {
      d = DateUtils.short2Date(date).getTime();
    } catch (ParseException e) {
      throw new FormulaException(e);
    }
    if (this.lowerDate > DEFAULT_TIMESTAMP && this.upperDate > DEFAULT_TIMESTAMP) {
      return d >= this.lowerDate && d < this.upperDate ? this.scaleValue : null;
    }
    if (this.lowerDate > DEFAULT_TIMESTAMP && this.upperDate == DEFAULT_TIMESTAMP) {
      return d >= this.lowerDate ? this.scaleValue : null;
    }
    if (this.upperDate > DEFAULT_TIMESTAMP && this.lowerDate == DEFAULT_TIMESTAMP) {
      return d < this.upperDate ? this.scaleValue : null;
    }
    return null;
  }

  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("s=");
    sb.append(scaleValue);
    sb.append("@[");
    if (this.lowerDate > DEFAULT_TIMESTAMP) {
      sb.append(this.lowerDateString);
    }
    sb.append(',');
    if (this.upperDate > DEFAULT_TIMESTAMP) {
      sb.append(this.upperDateString);
    }
    sb.append(']');
    return sb.toString();
  }
}
