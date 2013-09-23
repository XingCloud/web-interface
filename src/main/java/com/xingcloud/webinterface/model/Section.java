package com.xingcloud.webinterface.model;

import java.util.Comparator;

public class Section {
  private long min;
  private long max;
  private long accumulate;

  public Section() {
    super();
  }

  public Section(long min, long max) {
    this.min = min;
    this.max = max;
  }

  public String toString() {
    return "Min." + min + ".Max." + max + ".Accu." + accumulate;
  }

  public String name() {
    return min + "-" + max;
  }

  public long getMin() {
    return min;
  }

  public void setMin(long min) {
    this.min = min;
  }

  public long getMax() {
    return max;
  }

  public void setMax(long max) {
    this.max = max;
  }

  public long getAccumulate() {
    return accumulate;
  }

  public void setAccumulate(long accumulate) {
    this.accumulate = accumulate;
  }

  public void accumulate(long l) {
    this.accumulate += l;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (max ^ (max >>> 32));
    result = prime * result + (int) (min ^ (min >>> 32));
    return result;
  }

  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Section other = (Section) obj;
    if (max != other.max)
      return false;
    if (min != other.min)
      return false;
    return true;
  }

  public boolean thisNumberIsMine(long v) {
    return v >= min && v <= max;
  }

  public static class KeyAscComparator implements Comparator<Section> {
    public int compare(Section o1, Section o2) {
      if (o1 == null || o2 == null) {
        return 0;
      }

      long minThis = o1.getMin();
      long minThat = o2.getMin();
      long maxThis = o1.getMax();
      long maxThat = o2.getMax();

      if (minThis < minThat) {
        return -1;
      } else if (minThis > minThat) {
        return 1;
      } else {
        if (maxThis < maxThat) {
          return -1;
        } else if (maxThis > maxThat) {
          return 1;
        }
      }
      return 0;
    }
  }

  public static class KeyDescComparator implements Comparator<Section> {
    public int compare(Section o1, Section o2) {
      return -new KeyAscComparator().compare(o1, o2);
    }
  }

  public static class ValueAscComparator implements Comparator<Section> {
    public int compare(Section o1, Section o2) {
      if (o1 == null || o2 == null) {
        return 0;
      }
      double d1 = o1.getAccumulate();
      double d2 = o2.getAccumulate();

      if (d1 > d2) {
        return 1;
      } else if (d1 == d2) {
        return 0;
      } else {
        return -1;
      }
    }
  }

  public static class ValueDescComparator implements Comparator<Section> {
    public int compare(Section o1, Section o2) {
      return -new ValueAscComparator().compare(o1, o2);
    }
  }

}
