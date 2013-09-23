package com.xingcloud.webinterface.calculate;

import com.google.common.base.Objects;

/**
 * User: Z J Wu Date: 13-5-22 Time: 下午7:05 Package: com.xingcloud.webinterface.calculate
 */
public class Arity {

  private final String name;

  private final Object value;

  public Arity(String name, Object value) {
    this.name = name;
    this.value = value;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name, value);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final Arity other = (Arity) obj;
    return Objects.equal(this.name, other.name) && Objects.equal(this.value, other.value);
  }

  public String getName() {
    return name;
  }

  public Object getValue() {
    return value;
  }

  @Override
  public String toString() {
    return "{" + name + ", " + value + "}";
  }
}
