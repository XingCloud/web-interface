package com.xingcloud.webinterface.model;

/**
 * User: Z J Wu Date: 13-8-5 Time: 下午3:01 Package: com.xingcloud.webinterface.model
 */
public class EventPart {
  private String event;
  private byte location;

  public EventPart(String event, byte location) {
    this.event = event;
    this.location = location;
  }

  public String getEvent() {
    return event;
  }

  public byte getLocation() {
    return location;
  }

  @Override public String toString() {
    return event + "@" + location;
  }
}
