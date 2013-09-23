package com.xingcloud.webinterface.model.result;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import com.xingcloud.webinterface.enums.ErrorCode;

public class ErrorMiscResult extends MiscResult {

  @Expose
  @SerializedName("err_code")
  private ErrorCode errorCode;

  @Expose
  @SerializedName("err_msg")
  private String errorMessage;

  public ErrorMiscResult(boolean result, long milli, ErrorCode errorCode, String errorMessage) {
    super(result, milli);
    this.errorCode = errorCode;
    this.errorMessage = errorMessage;
  }

  public ErrorCode getErrorCode() {
    return errorCode;
  }

  public void setErrorCode(ErrorCode errorCode) {
    this.errorCode = errorCode;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public void setErrorMessage(String errorMessage) {
    this.errorMessage = errorMessage;
  }

}
