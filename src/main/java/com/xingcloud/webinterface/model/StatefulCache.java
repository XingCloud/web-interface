package com.xingcloud.webinterface.model;

import com.xingcloud.webinterface.enums.CacheReference;
import com.xingcloud.webinterface.enums.CacheState;

import java.util.Map;

public class StatefulCache {

  private CacheReference reference;

  private CacheState state;

  private Map<Object, ResultTuple> content;

  private long timeElapse;

  public StatefulCache(CacheReference reference, CacheState state, Map<Object, ResultTuple> content, long timeElapse) {
    super();
    this.reference = reference;
    this.state = state;
    this.content = content;
    this.timeElapse = timeElapse;
  }

  public CacheState getState() {
    return state;
  }

  public void setState(CacheState state) {
    this.state = state;
  }

  public Map<Object, ResultTuple> getContent() {
    return content;
  }

  public void setContent(Map<Object, ResultTuple> content) {
    this.content = content;
  }

  public long getTimeElapse() {
    return timeElapse;
  }

  public void setTimeElapse(long timeElapse) {
    this.timeElapse = timeElapse;
  }

  public CacheReference getReference() {
    return reference;
  }

  public void setReference(CacheReference reference) {
    this.reference = reference;
  }

  @Override
  public String toString() {
    return "StatefulCache [state=" + state + ", content=" + content + ", timeElapse=" + timeElapse + "]";
  }

}
