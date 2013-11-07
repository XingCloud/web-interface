package com.xingcloud.webinterface.sql;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import com.xingcloud.webinterface.enums.Operator;
import com.xingcloud.webinterface.enums.SegmentTableType;
import com.xingcloud.webinterface.utils.WebInterfaceConstants;

import java.util.Map;

/**
 * User: Z J Wu Date: 13-11-6 Time: 上午10:37 Package: com.xingcloud.webinterface.sql
 */
public class SegmentDescriptor {

  @Expose
  @SerializedName("t")
  private SegmentTableType type;
  @Expose
  @SerializedName("l")
  private Map<String, Map<Operator, Object>> content1;
  @Expose
  @SerializedName("r")
  private Map<String, Map<Operator, Object>> content2;

  public SegmentDescriptor(SegmentTableType type, Map<String, Map<Operator, Object>> content1) {
    this.type = type;
    this.content1 = content1;
  }

  public SegmentDescriptor(SegmentTableType type, Map<String, Map<Operator, Object>> content1,
                           Map<String, Map<Operator, Object>> content2) {
    this.type = type;
    this.content1 = content1;
    this.content2 = content2;
  }

  public SegmentTableType getType() {
    return type;
  }

  public Map<String, Map<Operator, Object>> getContent1() {
    return content1;
  }

  public Map<String, Map<Operator, Object>> getContent2() {
    return content2;
  }

  @Override public String toString() {
    return WebInterfaceConstants.DEFAULT_SQL_GSON_PLAIN.toJson(this);
  }
}
