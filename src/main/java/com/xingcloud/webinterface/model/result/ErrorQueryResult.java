package com.xingcloud.webinterface.model.result;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

import java.util.Map;

public class ErrorQueryResult extends QueryResult {

  @Expose
  @SerializedName("err_code")
  private String errorCode;

  @Expose
  @SerializedName("err_msg")
  private String errorMessage;

  public ErrorQueryResult(Map<String, Map<Object, Number>> mapData) {
    super(mapData);
  }

  public ErrorQueryResult(String errorCode, String errorMessage) {
    super();
    this.result = false;
    this.errorCode = errorCode;
    this.errorMessage = errorMessage;
  }

  public ErrorQueryResult(Map<String, Map<Object, Number>> mapData, String errorCode, String errorMessage) {
    super(mapData);
    this.errorCode = errorCode;
    this.errorMessage = errorMessage;
  }

  public String getErrorCode() {
    return errorCode;
  }

  public void setErrorCode(String errorCode) {
    this.errorCode = errorCode;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public void setErrorMessage(String errorMessage) {
    this.errorMessage = errorMessage;
  }

  protected void format() {

  }

}
