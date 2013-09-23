package com.xingcloud.webinterface.model;

import java.io.Serializable;

public class Pair implements Serializable {
  private static final long serialVersionUID = -4723444751153587358L;
  private Object k;
  private Object v;

  public Pair(Object k) {
    super();
    this.k = k;
  }

  public Pair(Object k, Object v) {
    super();
    this.k = k;
    this.v = v;
  }

  public Object getK() {
    return k;
  }

  public void setK(Object k) {
    this.k = k;
  }

  public Object getV() {
    return v;
  }

  public void setV(Object v) {
    this.v = v;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((k == null) ? 0 : k.hashCode());
    result = prime * result + ((v == null) ? 0 : v.hashCode());
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
    Pair other = (Pair) obj;
    if (k == null) {
      if (other.k != null)
        return false;
    } else if (!k.equals(other.k))
      return false;
    if (v == null) {
      if (other.v != null)
        return false;
    } else if (!v.equals(other.v))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "(" + k + ", " + v + ")";
  }

}
