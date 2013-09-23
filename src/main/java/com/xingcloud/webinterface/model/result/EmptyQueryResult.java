package com.xingcloud.webinterface.model.result;

import com.google.gson.annotations.Expose;
import com.xingcloud.webinterface.annotation.Ignore;

import java.util.Map;

public class EmptyQueryResult extends QueryResult {
  @Ignore
  public static final QueryResult INSTANCE = new EmptyQueryResult();

  @Expose
  private Object datas;

  public EmptyQueryResult() {
    super();
    this.mapData = null;
    this.result = true;
  }

  public EmptyQueryResult(Map<String, Map<Object, Number>> mapData) {
    super(mapData);
  }

  @Override
  protected void format() {

  }

  public Object getDatas() {
    return datas;
  }

  public void setDatas(Object datas) {
    this.datas = datas;
  }

}
