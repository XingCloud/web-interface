package com.xingcloud.webinterface.model.mongo;

import com.google.common.base.Strings;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public class Event implements Serializable, Writable {

  private static final long serialVersionUID = -2532185947507677322L;

  private String l0;
  private String l1;
  private String l2;
  private String l3;
  private String l4;
  private String l5;

  public Event() {
    super();
  }

  public Event(String l0, String l1, String l2, String l3, String l4, String l5) {
    super();
    this.l0 = l0;
    this.l1 = l1;
    this.l2 = l2;
    this.l3 = l3;
    this.l4 = l4;
    this.l5 = l5;
  }

  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((l0 == null) ? 0 : l0.hashCode());
    result = prime * result + ((l1 == null) ? 0 : l1.hashCode());
    result = prime * result + ((l2 == null) ? 0 : l2.hashCode());
    result = prime * result + ((l3 == null) ? 0 : l3.hashCode());
    result = prime * result + ((l4 == null) ? 0 : l4.hashCode());
    result = prime * result + ((l5 == null) ? 0 : l5.hashCode());
    return result;
  }

  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Event other = (Event) obj;
    if (l0 == null) {
      if (other.l0 != null)
        return false;
    } else if (!l0.equals(other.l0))
      return false;
    if (l1 == null) {
      if (other.l1 != null)
        return false;
    } else if (!l1.equals(other.l1))
      return false;
    if (l2 == null) {
      if (other.l2 != null)
        return false;
    } else if (!l2.equals(other.l2))
      return false;
    if (l3 == null) {
      if (other.l3 != null)
        return false;
    } else if (!l3.equals(other.l3))
      return false;
    if (l4 == null) {
      if (other.l4 != null)
        return false;
    } else if (!l4.equals(other.l4))
      return false;
    if (l5 == null) {
      if (other.l5 != null)
        return false;
    } else if (!l5.equals(other.l5))
      return false;
    return true;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    append(sb, l0);
    append(sb, l1);
    append(sb, l2);
    append(sb, l3);
    append(sb, l4);
    append(sb, l5);
    return sb.toString();
  }

  private void append(StringBuilder sb, String s) {
    if (Strings.isNullOrEmpty(s)) {
      return;
    }
    sb.append(s);
    sb.append('.');
  }

  public static void main(String[] args) {
    Event event = new Event(null, "b", "c", "d", "e", "f");
    System.out.println(event);
  }

  public void readFields(DataInput arg0) throws IOException {
    String l0 = WritableUtils.readCompressedString(arg0);
    String l1 = WritableUtils.readCompressedString(arg0);
    String l2 = WritableUtils.readCompressedString(arg0);
    String l3 = WritableUtils.readCompressedString(arg0);
    String l4 = WritableUtils.readCompressedString(arg0);
    String l5 = WritableUtils.readCompressedString(arg0);
    this.l0 = l0;
    this.l1 = l1;
    this.l2 = l2;
    this.l3 = l3;
    this.l4 = l4;
    this.l5 = l5;
  }

  public void write(DataOutput arg0) throws IOException {
    WritableUtils.writeCompressedString(arg0, l0);
    WritableUtils.writeCompressedString(arg0, l1);
    WritableUtils.writeCompressedString(arg0, l2);
    WritableUtils.writeCompressedString(arg0, l3);
    WritableUtils.writeCompressedString(arg0, l4);
    WritableUtils.writeCompressedString(arg0, l5);
  }

}
