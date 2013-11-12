package com.xingcloud.webinterface.sql;

/**
 * User: Z J Wu Date: 13-11-5 Time: 下午4:32 Package: com.xingcloud.webinterface.sql
 */
public class SqlUtilsConstants {
  public static final String EVENT_TABLE_PREFIX = "e.";
  public static final String USER_TABLE_PREFIX = "u.";
  public static final String SEGMENT_TOSTRING_BEGIN = "s{{";
  public static final String SEGMENT_TOSTRING_END = "}}s";
  public static final String USING_SGMT_FUNC = "\1\2\3\6";
  public static final String DATE_FIELD = "date";
  public static final String EVENT_FIELD = "event";
  public static final char SQL_CONDITION_SEPARATOR = ';';

  public static boolean isEventField(String fieldName) {
    return fieldName.contains(EVENT_FIELD);
  }

  public static boolean isDateField(String fieldName) {
    return fieldName.contains(DATE_FIELD);
  }

  public static boolean isEventTable(String tableName) {
    return tableName.contains("deu") || tableName.contains("event");
  }
}
