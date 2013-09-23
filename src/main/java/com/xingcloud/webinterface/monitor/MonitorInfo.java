package com.xingcloud.webinterface.monitor;

import com.xingcloud.webinterface.conf.WebInterfaceConfig;
import org.apache.commons.configuration.Configuration;

public class MonitorInfo {

  public static MonitorInfo MI_QUERY_ENTER;

  public static MonitorInfo MI_DESCRIPTOR_BUILD;

  public static MonitorInfo MI_REPORT_MIN5;
  public static MonitorInfo MI_REPORT_HOUR;
  public static MonitorInfo MI_REPORT_DAY;
  public static MonitorInfo MI_REPORT_WEEK;
  public static MonitorInfo MI_REPORT_MONTH;

  public static MonitorInfo MI_EXCEPTION_XPARAMETER_EXCEPTION;
  public static MonitorInfo MI_EXCEPTION_PARSE_JSON_EXCEPTION;
  public static MonitorInfo MI_EXCEPTION_INTERRUPT_QUERY_EXCEPTION;
  public static MonitorInfo MI_EXCEPTION_PARSE_INCREMENTAL_EXCEPTION;
  public static MonitorInfo MI_EXCEPTION_NUMBER_OF_DAY_EXCEPTION;
  public static MonitorInfo MI_EXCEPTION_XQUERY_EXCEPTION;
  public static MonitorInfo MI_EXCEPTION_SEGMENT_EXCEPTION;
  public static MonitorInfo MI_EXCEPTION_PARSE_EXCEPTION;
  public static MonitorInfo MI_EXCEPTION_RANGING_EXCEPTION;
  public static MonitorInfo MI_EXCEPTION_DATA_FILL_EXCEPTION;
  public static MonitorInfo MI_EXCEPTION_UI_CHECK_EXCEPTION;
  public static MonitorInfo MI_EXCEPTION_EXCEPTION;

  public static String MI_EXCEPTION_PREFIX;

  public static String MI_STR_DESCRIPTOR_MERGE;

  public static String MI_STR_TIME_USE_UI_CHECK;
  public static String MI_STR_TIME_USE_WHOLE_QUERY;
  public static String MI_STR_TIME_USE_BUILD_DESCRIPTOR;
  public static String MI_STR_TIME_USE_CHECK_CACHE;
  public static String MI_STR_TIME_USE_INTERGRATE_RESULT;
  public static String MI_STR_TIME_USE_CALCULATE;

  public static String MI_STR_CACHE_OFFLINE;
  public static String MI_STR_CACHE_ONLINE;

  static {
    Configuration configuration = WebInterfaceConfig.getConfiguration();
    MI_QUERY_ENTER = new MonitorInfo(configuration.getString("system-monitor.events.query-enter"));
    MI_REPORT_MIN5 = new MonitorInfo(configuration.getString("system-monitor.events.report.min5"));
    MI_REPORT_HOUR = new MonitorInfo(configuration.getString("system-monitor.events.report.hour"));
    MI_REPORT_DAY = new MonitorInfo(configuration.getString("system-monitor.events.report.day"));
    MI_REPORT_WEEK = new MonitorInfo(configuration.getString("system-monitor.events.report.week"));
    MI_REPORT_MONTH = new MonitorInfo(configuration.getString("system-monitor.events.report.month"));

    MI_DESCRIPTOR_BUILD = new MonitorInfo(configuration.getString("system-monitor.events.descriptor.build"));

    MI_EXCEPTION_XPARAMETER_EXCEPTION = new MonitorInfo(
      configuration.getString("system-monitor.events.exception.XParameterException"));
    MI_EXCEPTION_PARSE_JSON_EXCEPTION = new MonitorInfo(
      configuration.getString("system-monitor.events.exception.ParseJsonException"));
    MI_EXCEPTION_INTERRUPT_QUERY_EXCEPTION = new MonitorInfo(
      configuration.getString("system-monitor.events.exception.InterruptQueryException"));
    MI_EXCEPTION_PARSE_INCREMENTAL_EXCEPTION = new MonitorInfo(
      configuration.getString("system-monitor.events.exception.ParseIncrementalException"));
    MI_EXCEPTION_NUMBER_OF_DAY_EXCEPTION = new MonitorInfo(
      configuration.getString("system-monitor.events.exception.NumberOfDayException"));
    MI_EXCEPTION_XQUERY_EXCEPTION = new MonitorInfo(
      configuration.getString("system-monitor.events.exception.XQueryException"));
    MI_EXCEPTION_SEGMENT_EXCEPTION = new MonitorInfo(
      configuration.getString("system-monitor.events.exception.SegmentException"));
    MI_EXCEPTION_PARSE_EXCEPTION = new MonitorInfo(
      configuration.getString("system-monitor.events.exception.ParseException"));
    MI_EXCEPTION_RANGING_EXCEPTION = new MonitorInfo(
      configuration.getString("system-monitor.events.exception.RangingException"));
    MI_EXCEPTION_DATA_FILL_EXCEPTION = new MonitorInfo(
      configuration.getString("system-monitor.events.exception.DataFillException"));
    MI_EXCEPTION_UI_CHECK_EXCEPTION = new MonitorInfo(
      configuration.getString("system-monitor.events.exception.UICheckException"));
    MI_EXCEPTION_EXCEPTION = new MonitorInfo(configuration.getString("system-monitor.events.exception.Exception"));
    MI_EXCEPTION_PREFIX = configuration.getString("system-monitor.events.exception");
    MI_STR_DESCRIPTOR_MERGE = configuration.getString("system-monitor.events.descriptor.merge");
    MI_STR_TIME_USE_WHOLE_QUERY = configuration.getString("system-monitor.events.time_use.whole-query");
    MI_STR_TIME_USE_BUILD_DESCRIPTOR = configuration.getString("system-monitor.events.time_use.build-descriptor");
    MI_STR_TIME_USE_CHECK_CACHE = configuration.getString("system-monitor.events.time_use.check-cache");
    MI_STR_TIME_USE_INTERGRATE_RESULT = configuration.getString("system-monitor.events.time_use.intergrate-result");
    MI_STR_TIME_USE_CALCULATE = configuration.getString("system-monitor.events.time_use.calculate");
    MI_STR_TIME_USE_UI_CHECK = configuration.getString("system-monitor.events.time_use.ui-check");
    MI_STR_CACHE_OFFLINE = configuration.getString("system-monitor.events.cache.offline");
    MI_STR_CACHE_ONLINE = configuration.getString("system-monitor.events.cache.online");
  }

  private String event;

  private Long eventValue;

  public MonitorInfo(String event) {
    super();
    this.event = event;
  }

  public MonitorInfo(String event, Long eventValue) {
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
