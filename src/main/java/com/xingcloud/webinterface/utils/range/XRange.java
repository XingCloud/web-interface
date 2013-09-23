package com.xingcloud.webinterface.utils.range;

import static com.xingcloud.basic.Constants.INTERNAL_NA;
import static com.xingcloud.webinterface.enums.SliceType.NUMERIC;
import static com.xingcloud.webinterface.utils.range.RangeFromatter.format;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.xingcloud.webinterface.enums.SliceType;

public class XRange<T extends Comparable<T>> {

  public static final XRange<Long> INTERNAL_NA_LONG_XRANGE = new XRange<Long>(Integer.MAX_VALUE, INTERNAL_NA);

  private Range<T> range;

  private BoundType lowerBoundType;

  private T lowerEndPoint;

  private BoundType upperBoundType;

  private T upperEndPoint;

  private int position;

  private String formattedString;

  private SliceType type;

  public XRange() {
  }

  public XRange(int position, String formattedString) {
    super();
    this.position = position;
    this.formattedString = formattedString;
  }

  public XRange(Range<T> range, int position, SliceType type) {
    super();
    this.range = range;
    if (range != null) {
      if (range.hasLowerBound()) {
        lowerBoundType = range.lowerBoundType();
        lowerEndPoint = range.lowerEndpoint();
      }
      if (range.hasUpperBound()) {
        upperBoundType = range.upperBoundType();
        upperEndPoint = range.upperEndpoint();
      }
    }
    this.position = position;
    this.type = type;
    this.formattedString = getHumanFormat();
  }

  public Range<T> getRange() {
    return range;
  }

  public void setRange(Range<T> range) {
    this.range = range;
  }

  public BoundType getLowerBoundType() {
    return lowerBoundType;
  }

  public void setLowerBoundType(BoundType lowerBoundType) {
    this.lowerBoundType = lowerBoundType;
  }

  public T getLowerEndPoint() {
    return lowerEndPoint;
  }

  public void setLowerEndPoint(T lowerEndPoint) {
    this.lowerEndPoint = lowerEndPoint;
  }

  public BoundType getUpperBoundType() {
    return upperBoundType;
  }

  public void setUpperBoundType(BoundType upperBoundType) {
    this.upperBoundType = upperBoundType;
  }

  public T getUpperEndPoint() {
    return upperEndPoint;
  }

  public void setUpperEndPoint(T upperEndPoint) {
    this.upperEndPoint = upperEndPoint;
  }

  public int getPosition() {
    return position;
  }

  public void setPosition(int position) {
    this.position = position;
  }

  public String getFormattedString() {
    return formattedString;
  }

  public void setFormattedString(String formattedString) {
    this.formattedString = formattedString;
  }

  public SliceType getType() {
    return type;
  }

  public void setType(SliceType type) {
    this.type = type;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((lowerBoundType == null) ? 0 : lowerBoundType.hashCode()
    );
    result = prime * result + ((lowerEndPoint == null) ? 0 : lowerEndPoint.hashCode()
    );
    result = prime * result + position;
    result = prime * result + ((range == null) ? 0 : range.hashCode());
    result = prime * result + ((type == null) ? 0 : type.hashCode());
    result = prime * result + ((upperBoundType == null) ? 0 : upperBoundType.hashCode()
    );
    result = prime * result + ((upperEndPoint == null) ? 0 : upperEndPoint.hashCode()
    );
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
    XRange other = (XRange) obj;
    if (lowerBoundType != other.lowerBoundType)
      return false;
    if (lowerEndPoint == null) {
      if (other.lowerEndPoint != null)
        return false;
    } else if (!lowerEndPoint.equals(other.lowerEndPoint))
      return false;
    if (position != other.position)
      return false;
    if (range == null) {
      if (other.range != null)
        return false;
    } else if (!range.equals(other.range))
      return false;
    if (type != other.type)
      return false;
    if (upperBoundType != other.upperBoundType)
      return false;
    if (upperEndPoint == null) {
      if (other.upperEndPoint != null)
        return false;
    } else if (!upperEndPoint.equals(other.upperEndPoint))
      return false;
    return true;
  }

  public String getHumanFormat() {
    boolean hasLower = (this.lowerBoundType != null);
    boolean hasUpper = (this.upperBoundType != null);

    if (!(hasLower || hasUpper)) {
      return null;
    }

    StringBuilder sb = new StringBuilder();
    if (!hasLower) {
      sb.append('<');
      sb.append(format(range.upperEndpoint(), type));
      return sb.toString();
    }
    if (!hasUpper) {
      sb.append('≥');
      sb.append(format(range.lowerEndpoint(), type));
      return sb.toString();
    }

    if (this.lowerEndPoint.equals(this.upperEndPoint)) {
      sb.append(format(this.lowerEndPoint, type));
      return sb.toString();
    }

    sb.append(format(lowerEndPoint, type));
    sb.append(" - ");
    sb.append(format(upperEndPoint, type));
    return sb.toString();
  }

  public boolean contains(T t) {
    return range.contains(t);
  }

  public String toString() {
    // System.out.println("XRange." + position + ".(" + getFormattedString()
    // + ")");
    return getFormattedString();
  }

  public static void main(String[] args) {
    XRange<Long> range1 = new XRange<Long>(Range.closed(1l, 2l), 0, NUMERIC);
    XRange<Long> range2 = new XRange<Long>(Range.closed(1l, 2l), 1, NUMERIC);
    System.out.println(range1.hashCode());
    System.out.println(range2.hashCode());
    System.out.println(range1.equals(range2));
  }

}
