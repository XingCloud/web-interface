package com.xingcloud.webinterface.model.result;

import com.google.gson.annotations.Expose;

public class MiscResult {

  @Expose
  protected boolean result;

  @Expose
  protected long milli;

  public MiscResult(boolean result, long milli) {
    super();
    this.result = result;
    this.milli = milli;
  }

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

}
