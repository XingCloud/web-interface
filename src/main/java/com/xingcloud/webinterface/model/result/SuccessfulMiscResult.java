package com.xingcloud.webinterface.model.result;

import com.google.gson.annotations.Expose;

public class SuccessfulMiscResult extends MiscResult {

  @Expose
  private Object data;

  public SuccessfulMiscResult(boolean result, long milli, Object data) {
    super(result, milli);
    this.data = data;
  }

  public Object getData() {
    return data;
  }

  public void setData(Object data) {
    this.data = data;
  }

}
