package com.xingcloud.webinterface.sql.visitor;

import static com.xingcloud.mysql.PropType.sql_bigint;
import static com.xingcloud.mysql.PropType.sql_datetime;
import static com.xingcloud.webinterface.calculate.func.DateAddFunction.DATE_ADD_FUNCTION_NAME;
import static com.xingcloud.webinterface.enums.Operator.EQ;
import static com.xingcloud.webinterface.enums.Operator.GE;
import static com.xingcloud.webinterface.enums.Operator.GT;
import static com.xingcloud.webinterface.enums.Operator.IN;
import static com.xingcloud.webinterface.enums.Operator.LE;
import static com.xingcloud.webinterface.enums.Operator.LT;
import static com.xingcloud.webinterface.enums.SegmentTableType.E;
import static com.xingcloud.webinterface.enums.SegmentTableType.U;
import static com.xingcloud.webinterface.sql.SqlUtilsConstants.USING_SGMT_FUNC;
import static com.xingcloud.webinterface.sql.SqlUtilsConstants.isDateField;

import com.xingcloud.mysql.PropType;
import com.xingcloud.mysql.UserProp;
import com.xingcloud.webinterface.enums.Interval;
import com.xingcloud.webinterface.enums.Operator;
import com.xingcloud.webinterface.enums.SegmentTableType;
import com.xingcloud.webinterface.exception.ExpressionEvaluationException;
import com.xingcloud.webinterface.exception.SegmentException;
import com.xingcloud.webinterface.exception.UserPropertyException;
import com.xingcloud.webinterface.model.formula.CommonFormulaQueryDescriptor;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import com.xingcloud.webinterface.utils.UserPropertiesInfoManager;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.schema.Column;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * User: Z J Wu Date: 13-10-30 Time: 上午9:54 Package: com.xingcloud.webinterface.sql.visitor
 */
public class SegmentExpressionVisitor extends AbstractExprVisitor {
  public static final Map<String, Operator> OPERATOR_MAP = new HashMap<String, Operator>();

  static {
    OPERATOR_MAP.put("=", EQ);
    OPERATOR_MAP.put(">=", GE);
    OPERATOR_MAP.put(">", GT);
    OPERATOR_MAP.put("<=", LE);
    OPERATOR_MAP.put("<", LT);
    OPERATOR_MAP.put("in", IN);
  }

  private SegmentTableType segmentTableType;

  private Map<String, Map<Operator, Object>> conditionMap;

  private Object conditionValue;

  public SegmentExpressionVisitor(FormulaQueryDescriptor descriptor, SegmentTableType segmentTableType,
                                  Map<String, Map<Operator, Object>> conditionMap) {
    super(descriptor);
    this.segmentTableType = segmentTableType;
    this.conditionMap = conditionMap;
  }

  public Object getConditionValue() {
    return conditionValue;
  }

  @Override public void visit(Function function) {
    String funcName = function.getName();
    if (DATE_ADD_FUNCTION_NAME.equals(funcName)) {
      if (U.equals(this.segmentTableType) && descriptor.isCommon()) {
        CommonFormulaQueryDescriptor cfqd = (CommonFormulaQueryDescriptor) descriptor;
        Interval interval = cfqd.getInterval();
        if (interval.getDays() < 1) {
          this.conditionValue = USING_SGMT_FUNC;
          return;
        }
      }
      try {
        this.conditionValue = visitDateAddFunction(function);
      } catch (ExpressionEvaluationException e) {
        this.exception = e;
        return;
      }
    } else {
      this.exception = new SegmentException("Unsupported function - " + funcName);
    }
  }

  @Override public void visit(Column column) {
    String columnName = column.getColumnName();
    Map<Operator, Object> valueMap = conditionMap.get(columnName);
    if (valueMap == null) {
      conditionMap.put(columnName, new TreeMap<Operator, Object>());
    }
  }

  @Override public void visit(MinorThanEquals minorThanEquals) {
    visitMathBinaryExpression(minorThanEquals);
  }

  @Override public void visit(MinorThan minorThan) {
    visitMathBinaryExpression(minorThan);
  }

  @Override public void visit(InExpression inExpression) {
    Column userProperty = (Column) inExpression.getLeftExpression();
    String userPropName = userProperty.getColumnName();
    UserProp userProp;
    try {
      userProp = UserPropertiesInfoManager.getInstance().getUserProp(descriptor.getProjectId(), userPropName);
    } catch (UserPropertyException e) {
      this.exception = e;
      return;
    }
    SegmentItemsListVisitor visitor = new SegmentItemsListVisitor(descriptor, userProp);
    inExpression.getItemsList().accept(visitor);
    if (visitor.isWrong()) {
      this.exception = visitor.getException();
      return;
    }
    Set<Object> values = visitor.getValues();
    putMapValue(userPropName, IN, values);
  }

  @Override public void visit(GreaterThanEquals greaterThanEquals) {
    visitMathBinaryExpression(greaterThanEquals);
  }

  @Override public void visit(GreaterThan greaterThan) {
    visitMathBinaryExpression(greaterThan);
  }

  @Override public void visit(EqualsTo equalsTo) {
    visitMathBinaryExpression(equalsTo);
  }

  @Override public void visit(OrExpression orExpression) {
    this.exception = new SegmentException("\"OR\" operator is not accepted in deu where conditions.");
  }

  @Override public void visit(AndExpression andExpression) {
    Expression leftExpr = andExpression.getLeftExpression();
    SegmentExpressionVisitor leftDEUExprVisitor = new SegmentExpressionVisitor(descriptor, this.segmentTableType,
                                                                               conditionMap);
    leftExpr.accept(leftDEUExprVisitor);
    if (leftDEUExprVisitor.isWrong()) {
      this.exception = leftDEUExprVisitor.getException();
      return;
    }

    Expression rightExpr = andExpression.getRightExpression();
    SegmentExpressionVisitor rightDEUExprVisitor = new SegmentExpressionVisitor(descriptor, this.segmentTableType,
                                                                                conditionMap);
    rightExpr.accept(rightDEUExprVisitor);
    if (rightDEUExprVisitor.isWrong()) {
      this.exception = rightDEUExprVisitor.getException();
    }
  }

  private void visitMathBinaryExpression(BinaryExpression binaryExpression) {
    String operatorName = binaryExpression.getStringExpression().toLowerCase();
    Operator operator = OPERATOR_MAP.get(operatorName);
    Expression leftExpr = binaryExpression.getLeftExpression();
    Expression rightExpr = binaryExpression.getRightExpression();
    Column column = (Column) leftExpr;
    String columnName = column.getColumnName();
    SegmentExpressionVisitor columnVisitor = new SegmentExpressionVisitor(descriptor, this.segmentTableType,
                                                                          conditionMap);
    column.accept(columnVisitor);
    if (columnVisitor.isWrong()) {
      this.exception = columnVisitor.getException();
      return;
    }

    Object conditionVal;
    String stringValue;
    if (isColumn(rightExpr)) {
      stringValue = ((Column) rightExpr).getColumnName();
    } else {
      SegmentExpressionVisitor visitor = new SegmentExpressionVisitor(descriptor, this.segmentTableType, conditionMap);
      rightExpr.accept(visitor);
      if (visitor.isWrong()) {
        this.exception = visitor.getException();
        return;
      }
      stringValue = visitor.getConditionValue().toString();
    }

    if (E.equals(this.segmentTableType)) {
      conditionVal = stringValue;
      if (EQ.equals(operator) && isDateField(columnName)) {
        putMapValue(columnName, GE, conditionVal);
        putMapValue(columnName, LE, conditionVal);
        return;
      }
    } else {
      UserProp up;
      try {
        up = UserPropertiesInfoManager.getInstance().getUserProp(descriptor.getProjectId(), columnName);
      } catch (UserPropertyException e) {
        this.exception = e;
        return;
      }
      PropType pt = up.getPropType();
      conditionVal = convertUserPropertyValue(pt, stringValue);
      if (EQ.equals(operator) && (sql_bigint.equals(pt) || sql_datetime.equals(pt))) {
        putMapValue(columnName, GE, conditionVal);
        putMapValue(columnName, LE, conditionVal);
        return;
      }
    }
    putMapValue(columnName, operator, conditionVal);
  }

  private void putMapValue(String columnName, Operator operator, Object conditionValue) {
    Map<Operator, Object> singleColumnMap = conditionMap.get(columnName);
    if (singleColumnMap == null) {
      singleColumnMap = new TreeMap<Operator, Object>();
      conditionMap.put(columnName, singleColumnMap);
    }
    singleColumnMap.put(operator, conditionValue);
  }

  private Object convertUserPropertyValue(PropType pt, String stringValue) {
    switch (pt) {
      case sql_bigint:
        return Long.valueOf(stringValue);
      default:
        return stringValue;
    }
  }

  @Override
  public void visit(StringValue stringValue) {
    this.conditionValue = stringValue.getValue();
  }

  @Override
  public void visit(DoubleValue doubleValue) {
    this.conditionValue = doubleValue.getValue();
  }

  @Override
  public void visit(LongValue longValue) {
    this.conditionValue = longValue.getValue();
  }
}
