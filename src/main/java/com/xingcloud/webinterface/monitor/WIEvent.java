package com.xingcloud.webinterface.monitor;

import com.xingcloud.webinterface.conf.WebInterfaceConfig;
import org.apache.commons.configuration.Configuration;

public class WIEvent {

  public static WIEvent WIE_QUERY_ENTER;

  public static String WIE_STR_DESCRIPTOR_BUILD;
  public static String WIE_STR_DESCRIPTOR_MERGE;

  public static String WIE_STR_CACHE_OFFLINE;
  public static String WIE_STR_CACHE_ONLINE;

  public static String WIE_STR_TIMEUSE_UI_CHECK;
  public static String WIE_STR_TIMEUSE_BUILD_DESCRIPTOR;
  public static String WIE_STR_TIMEUSE_CHECK_CACHE;
  public static String WIE_STR_TIMEUSE_INTEGRATE_RESULT;
  public static String WIE_STR_TIMEUSE_CALCULATE;
  public static String WIE_STR_TIMEUSE_WHOLE_QUERY;

  static {
    Configuration configuration = WebInterfaceConfig.getConfiguration();
    WIE_QUERY_ENTER = new WIEvent(configuration.getString("system-monitor.events.query-enter"));

    WIE_STR_DESCRIPTOR_BUILD = configuration.getString("system-monitor.events.descriptor.build");
    WIE_STR_DESCRIPTOR_MERGE = configuration.getString("system-monitor.events.descriptor.merge");

    WIE_STR_CACHE_OFFLINE = configuration.getString("system-monitor.events.cache.offline");
    WIE_STR_CACHE_ONLINE = configuration.getString("system-monitor.events.cache.online");

    WIE_STR_TIMEUSE_UI_CHECK = configuration.getString("system-monitor.events.time-use.ui-check");
    WIE_STR_TIMEUSE_BUILD_DESCRIPTOR = configuration.getString("system-monitor.events.time-use.build-descriptor");
    WIE_STR_TIMEUSE_CHECK_CACHE = configuration.getString("system-monitor.events.time-use.check-cache");
    WIE_STR_TIMEUSE_INTEGRATE_RESULT = configuration.getString("system-monitor.events.time-use.integrate-result");
    WIE_STR_TIMEUSE_CALCULATE = configuration.getString("system-monitor.events.time-use.calculate");
    WIE_STR_TIMEUSE_WHOLE_QUERY = configuration.getString("system-monitor.events.time-use.whole-query");
  }

  public static WIEvent buildDescriptorBuild(String projectId) {
    return new WIEvent(WIE_STR_DESCRIPTOR_BUILD + '.' + projectId);
  }

  public static WIEvent buildDescriptorMerge(String projectId) {
    return new WIEvent(WIE_STR_DESCRIPTOR_MERGE + '.' + projectId);
  }

  public static WIEvent buildOnlineCacheHit(String projectId) {
    return new WIEvent(WIE_STR_CACHE_OFFLINE + '.' + projectId);
  }

  public static WIEvent buildOfflineCacheHit(String projectId) {
    return new WIEvent(WIE_STR_CACHE_ONLINE + '.' + projectId);
  }

  private String event;

  private Long eventValue;

  public WIEvent(String event) {
    super();
    this.event = event;
  }

  public WIEvent(String event, Long eventValue) {
    super();
    this.event = event;
    this.eventValue = eventValue;
  }

  public String getEvent() {
    return event;
  }

  public void setEvent(String event) {
    this.event = event;
  }

  public Long getEventValue() {
    return eventValue;
  }

  public void setEventValue(Long eventValue) {
    this.eventValue = eventValue;
  }

  @Override
  public String toString() {
    if (eventValue == null) {
      return event;
    }
    return event + "," + eventValue;
  }

}
