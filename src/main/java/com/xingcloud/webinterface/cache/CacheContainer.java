package com.xingcloud.webinterface.cache;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CacheContainer {
  private String id;

  private Set<String> eventStringSet;
  private Set<Map<String, String>> eventMapSet;
  private Map<String, Set<String>> distinctedEventRowMap;

  public CacheContainer() {
    super();
  }

  public CacheContainer(String id, int capacity) {
    super();
    this.id = id;
    this.eventStringSet = new HashSet<String>(capacity);
    this.eventMapSet = new HashSet<Map<String, String>>(capacity);
    this.distinctedEventRowMap = new HashMap<String, Set<String>>(6);
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public Set<String> getEventStringSet() {
    return eventStringSet;
  }

  public void setEventStringSet(Set<String> eventStringSet) {
    this.eventStringSet = eventStringSet;
  }

  public Set<Map<String, String>> getEventMapSet() {
    return eventMapSet;
  }

  public void setEventMapSet(Set<Map<String, String>> eventMapSet) {
    this.eventMapSet = eventMapSet;
  }

  public Map<String, Set<String>> getDistinctedEventRowMap() {
    return distinctedEventRowMap;
  }

  public void setDistinctedEventRowMap(Map<String, Set<String>> distinctedEventRowMap) {
    this.distinctedEventRowMap = distinctedEventRowMap;
  }

  public synchronized Set<String> getDistinctedSet(String key) {
    return this.distinctedEventRowMap.get(key);
  }

  public synchronized void putDistinctedSet(String targetRow, Set<String> set) {
    this.distinctedEventRowMap.put(targetRow, set);
  }

  public synchronized String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[ID]:\t" + getId());
    sb.append('\n');
    sb.append("[EventMapSize]:\t" + this.eventMapSet.size());
    sb.append('\n');
    sb.append("[EventStringSize]:\t" + this.eventStringSet.size());
    sb.append('\n');
    for (String k : this.distinctedEventRowMap.keySet()) {
      sb.append("[DistinctRowSize]:\t[" + k + "]-" + this.distinctedEventRowMap.get(k).size());
      sb.append('\n');
    }
    return sb.toString();
  }
}
