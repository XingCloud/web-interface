package com.xingcloud.webinterface.calculate;

import com.xingcloud.basic.utils.DateUtils;
import com.xingcloud.webinterface.exception.FormulaException;

import java.text.ParseException;

/**
 * User: Z J Wu Date: 13-12-11 Time: 下午4:55 Package: com.xingcloud.webinterface.calculate
 */
public class Formula {
  private long lowerDate;
  private String lowerDateString;
  private long upperDate;
  private String upperDateString;
  private String formulaString;

  private Formula() {
  }

  public long getLowerDate() {
    return lowerDate;
  }

  public long getUpperDate() {
    return upperDate;
  }

  public String getFormulaString() {
    return formulaString;
  }

  public static Formula buildRangeFormula(String lowerDate, String upperDate, String formulaString) throws
    FormulaException {
    Formula formula = new Formula();
    try {
      formula.lowerDate = DateUtils.short2Date(lowerDate).getTime();
    } catch (ParseException e) {
      throw new FormulaException("Cannot parse lower date of formula(" + formulaString + ") - " + lowerDate);
    }
    try {
      formula.upperDate = DateUtils.short2Date(upperDate).getTime();
    } catch (ParseException e) {
      throw new FormulaException("Cannot parse upper date of formula(" + formulaString + ") - " + upperDate);
    }
    formula.lowerDateString = lowerDate;
    formula.upperDateString = upperDate;
    formula.formulaString = formulaString;
    return formula;
  }

  public static Formula buildLowerFormula(String lowerDate, String formulaString) throws FormulaException {
    Formula formula = new Formula();
    try {
      formula.lowerDate = DateUtils.short2Date(lowerDate).getTime();
    } catch (ParseException e) {
      throw new FormulaException("Cannot parse lower date of formula(" + formulaString + ") - " + lowerDate);
    }
    formula.lowerDateString = lowerDate;
    formula.formulaString = formulaString;
    return formula;
  }

  public static Formula buildDefaultFormula(String formulaString) throws FormulaException {
    Formula formula = new Formula();
    formula.formulaString = formulaString;
    return formula;
  }

  public String accept(String date) throws FormulaException {
    long d;
    try {
      d = DateUtils.short2Date(date).getTime();
    } catch (ParseException e) {
      throw new FormulaException(e);
    }
    if (this.lowerDate > 0 && this.upperDate > 0) {
      return d >= this.lowerDate && d < this.upperDate ? this.formulaString : null;
    }
    if (this.lowerDate > 0 && this.upperDate == 0) {
      return d >= this.lowerDate ? this.formulaString : null;
    }
    if (this.upperDate > 0 && this.lowerDate == 0) {
      return d < this.upperDate ? this.formulaString : null;
    }
    return null;
  }

  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("f=");
    sb.append(formulaString);
    sb.append("@[");
    if (this.lowerDate > 0) {
      sb.append(this.lowerDateString);
    }
    sb.append(',');
    if (this.upperDate > 0) {
      sb.append(this.upperDateString);
    }
    sb.append(']');
    return sb.toString();
  }
}
