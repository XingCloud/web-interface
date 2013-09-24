package com.xingcloud.webinterface.model.intermediate;

import com.xingcloud.webinterface.model.ResultTuple;

public class KeyTuple {
  private Object key;

  private Object additionalKey;

  private ResultTuple resultTuple;

  private Object status;

  public KeyTuple(Object key, ResultTuple resultTuple) {
    super();
    this.key = key;
    this.resultTuple = resultTuple;
  }

  public KeyTuple(Object key, ResultTuple resultTuple, Object status) {
    super();
    this.key = key;
    this.resultTuple = resultTuple;
    this.status = status;
    this.additionalKey = key;
  }

  public KeyTuple(Object key, Object additionalKey, ResultTuple resultTuple, Object status) {
    super();
    this.key = key;
    this.additionalKey = additionalKey;
    this.resultTuple = resultTuple;
    this.status = status;
  }

  public boolean hasValue() {
    return resultTuple != null;
  }

  public Object getStatus() {
    return status;
  }

  public void setStatus(Object status) {
    this.status = status;
  }

  public Object getKey() {
    return key;
  }

  public void setKey(Object key) {
    this.key = key;
  }

  public ResultTuple getResultTuple() {
    return resultTuple;
  }

  public void setResultTuple(ResultTuple resultTuple) {
    this.resultTuple = resultTuple;
  }

  public Object getAdditionalKey() {
    return additionalKey;
  }

  public void setAdditionalKey(Object additionalKey) {
    this.additionalKey = additionalKey;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((key == null) ? 0 : key.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    KeyTuple other = (KeyTuple) obj;
    if (key == null) {
      if (other.key != null)
        return false;
    } else if (!key.equals(other.key))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "(KT," + status + "," + key + "," + additionalKey + "," + resultTuple;
  }

}
