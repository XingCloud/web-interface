package com.xingcloud.webinterface.model;

import com.google.gson.annotations.Expose;

import java.io.Serializable;

public class Slice implements Serializable {

  private static final long serialVersionUID = -1353303536612607102L;

  @Expose
  private String key;

  @Expose
  private Number value;

  @Expose
  private double percent;

  public Slice() {
    super();
  }

  public Slice(String key, Number value, double percent) {
    this.key = key;
    this.value = value;
    this.percent = percent;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public Number getValue() {
    return value;
  }

  public void setValue(Number value) {
    this.value = value;
  }

  public double getPercent() {
    return percent;
  }

  public void setPercent(double percent) {
    this.percent = percent;
  }

  @Override
  public String toString() {
    return "Slice." + key + "." + value + "." + percent;
  }

}
