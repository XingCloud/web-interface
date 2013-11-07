package com.xingcloud.webinterface.sql.visitor;

import static com.xingcloud.webinterface.calculate.func.DateAddFunction.DATE_ADD_FUNCTION_NAME;
import static com.xingcloud.webinterface.calculate.func.DateAddFunction.DATE_ADD_FUNCTION_START_KEYWORD;

import com.xingcloud.webinterface.calculate.Arity;
import com.xingcloud.webinterface.calculate.Evaluator;
import com.xingcloud.webinterface.exception.ExpressionEvaluationException;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.ValueExpressions;

import java.util.ArrayList;
import java.util.List;

/**
 * User: Z J Wu Date: 13-10-28 Time: 下午5:40 Package: com.xingcloud.webinterface.sql.visitor
 */
public class AbstractExprVisitor extends FQDVisitor implements ExpressionVisitor {

  protected LogicalExpression logicalExpression;

  public LogicalExpression getLogicalExpression() {
    return logicalExpression;
  }

  public AbstractExprVisitor(FormulaQueryDescriptor descriptor) {
    super(descriptor);
  }

  @Override public void visit(NullValue nullValue) {
  }

  @Override public void visit(Function function) {
  }

  @Override public void visit(InverseExpression inverseExpression) {
  }

  @Override public void visit(JdbcParameter jdbcParameter) {
  }

  @Override public void visit(DoubleValue doubleValue) {
    this.logicalExpression = new ValueExpressions.DoubleExpression(doubleValue.getValue(), ExpressionPosition.UNKNOWN);
  }

  @Override public void visit(LongValue longValue) {
    this.logicalExpression = new ValueExpressions.LongExpression(longValue.getValue(), ExpressionPosition.UNKNOWN);
  }

  @Override public void visit(DateValue dateValue) {
  }

  @Override public void visit(TimeValue timeValue) {
  }

  @Override public void visit(TimestampValue timestampValue) {
  }

  @Override public void visit(Parenthesis parenthesis) {
  }

  @Override public void visit(StringValue stringValue) {
    this.logicalExpression = new ValueExpressions.QuotedString(stringValue.getValue(), ExpressionPosition.UNKNOWN);
  }

  @Override public void visit(Addition addition) {
  }

  @Override public void visit(Division division) {
  }

  @Override public void visit(Multiplication multiplication) {
  }

  @Override public void visit(Subtraction subtraction) {
  }

  @Override public void visit(AndExpression andExpression) {
  }

  @Override public void visit(OrExpression orExpression) {
  }

  @Override public void visit(Between between) {
  }

  @Override public void visit(EqualsTo equalsTo) {
  }

  @Override public void visit(GreaterThan greaterThan) {
  }

  @Override public void visit(GreaterThanEquals greaterThanEquals) {
  }

  @Override public void visit(InExpression inExpression) {
  }

  @Override public void visit(IsNullExpression isNullExpression) {
  }

  @Override public void visit(LikeExpression likeExpression) {
  }

  @Override public void visit(MinorThan minorThan) {
  }

  @Override public void visit(MinorThanEquals minorThanEquals) {
  }

  @Override public void visit(NotEqualsTo notEqualsTo) {
  }

  @Override public void visit(Column column) {
  }

  @Override public void visit(SubSelect subSelect) {
  }

  @Override public void visit(CaseExpression caseExpression) {
  }

  @Override public void visit(WhenClause whenClause) {
  }

  @Override public void visit(ExistsExpression existsExpression) {
  }

  @Override public void visit(AllComparisonExpression allComparisonExpression) {
  }

  @Override public void visit(AnyComparisonExpression anyComparisonExpression) {
  }

  @Override public void visit(Concat concat) {
  }

  @Override public void visit(Matches matches) {
  }

  @Override public void visit(BitwiseAnd bitwiseAnd) {
  }

  @Override public void visit(BitwiseOr bitwiseOr) {
  }

  @Override public void visit(BitwiseXor bitwiseXor) {
  }

  protected boolean columnIsLeft(Expression left, Expression right) {
    return isColumn(left) && !isColumn(right);
  }

  protected boolean isColumn(Expression expression) {
    return expression instanceof Column;
  }

  protected String visitDateAddFunction(Function function) throws ExpressionEvaluationException {
    ExpressionList parameters = function.getParameters();
    List parameterList = parameters.getExpressions();
    String formula;
    List<Arity> arity;
    String type = ((StringValue) parameterList.get(0)).getValue();
    formula = DATE_ADD_FUNCTION_NAME + "(date, " + parameterList.get(1) + ")";
    arity = new ArrayList<Arity>(1);
    if (DATE_ADD_FUNCTION_START_KEYWORD.equals(type)) {
      arity.add(new Arity("date", descriptor.getInputBeginDate()));
    } else {
      arity.add(new Arity("date", descriptor.getInputEndDate()));
    }
    return Evaluator.evaluate(formula, arity).toString();
  }
}
