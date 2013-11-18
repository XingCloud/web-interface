package com.xingcloud.webinterface.enums;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import java.rmi.activation.UnknownObjectException;
import java.util.EnumSet;
import java.util.Set;

public enum Operator {
  // Hour function
  SGMT("sgmt", "sgmt", true, true),
  // Hour function
  SGMT300("sgmt300", "sgmt300", true, true),
  // Min5 function
  SGMT3600("sgmt3600", "sgmt3600", true, true),
  // Do not filter anything.
  ALL(),
  // ==
  EQ("eq", "==", false, false),
  // >
  GT("gt", ">", false, false),
  // >=
  GE("ge", ">=", false, false),
  // <
  LT("lt", "<", false, false),
  // <=
  LE("le", "<=", false, false),
  IN("in", "in", false, true),
  // !=
  NE("ne", "<>", false, false);

  private static final Set<Operator> SET = EnumSet.allOf(Operator.class);
  public static final BiMap<Operator, String> OPERATOR_COMMON_KEYWORDS_BIMAP = HashBiMap.create();

  static {
    String keyword;
    for (Operator operator : SET) {
      keyword = operator.getId();
      if (keyword != null) {
        OPERATOR_COMMON_KEYWORDS_BIMAP.put(operator, keyword);
      }
    }
  }

  private String id;

  private String sqlOperator;

  private boolean isFunctional;

  private boolean needWhiteSpaceInToString;

  private Operator() {
  }

  private Operator(String id, String sqlOperator, boolean functional, boolean needWhiteSpaceInToString) {
    this.id = id;
    this.sqlOperator = sqlOperator;
    isFunctional = functional;
    this.needWhiteSpaceInToString = needWhiteSpaceInToString;
  }

  public boolean isFunctional() {
    return isFunctional;
  }

  public boolean needWhiteSpaceInToString() {
    return needWhiteSpaceInToString;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getSqlOperator() {
    return sqlOperator;
  }

  public void setSqlOperator(String sqlOperator) {
    this.sqlOperator = sqlOperator;
  }

  public void setFunctional(boolean functional) {
    isFunctional = functional;
  }

  public void setNeedWhiteSpaceInToString(boolean needWhiteSpaceInToString) {
    this.needWhiteSpaceInToString = needWhiteSpaceInToString;
  }

  public static String toSqlOperator(String id) throws UnknownObjectException {
    try {
      return OPERATOR_COMMON_KEYWORDS_BIMAP.inverse().get(id).getSqlOperator();
    } catch (Exception e) {
      throw new UnknownObjectException("Unknown common operation key word - " + id);
    }
  }

  public static void main(String[] args) throws UnknownObjectException {
    Set<Operator> set = EnumSet.allOf(Operator.class);
    for (Operator o : set) {
      System.out.println(o.getSqlOperator());
    }
  }

}
