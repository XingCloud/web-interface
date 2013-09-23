package com.xingcloud.webinterface.model.result;

import com.google.gson.annotations.Expose;

import java.util.Map;

public abstract class QueryResult {

  @Expose
  protected boolean result;

  @Expose
  protected long milli;

  protected Map<String, Map<Object, Number>> mapData;

  public QueryResult() {
    super();
  }

  public QueryResult(Map<String, Map<Object, Number>> mapData) {
    super();
    this.mapData = mapData;
    this.result = true;
  }

  protected abstract void format();

  public boolean isResult() {
    return result;
  }

  public void setResult(boolean result) {
    this.result = result;
  }

  public long getMilli() {
    return milli;
  }

  public void setMilli(long milli) {
    this.milli = milli;
  }

  public Map<String, Map<Object, Number>> getMapData() {
    return mapData;
  }

  public void setMapData(Map<String, Map<Object, Number>> mapData) {
    this.mapData = mapData;
  }

}
