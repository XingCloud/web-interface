package com.xingcloud.webinterface.calculate;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import com.xingcloud.webinterface.calculate.func.DateAddFunction;
import com.xingcloud.webinterface.exception.ExpressionEvaluationException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Evaluator {

  private static final Logger LOGGER = Logger.getLogger(Evaluator.class);

  private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("0.####");

  static {
    AviatorEvaluator.addFunction(new DateAddFunction());
  }

  public static Number evaluateNumber(String formula, List<Arity> arity) throws ExpressionEvaluationException {
    try {
      return ((Number) evaluate(formula, arity));
    } catch (ExpressionEvaluationException e) {
      throw e;
    } catch (Exception e) {
      throw new ExpressionEvaluationException(e);
    }
  }

  public static Object evaluateObject(String formula, List<Arity> arity) throws ExpressionEvaluationException {
    try {
      return evaluate(formula, arity);
    } catch (ExpressionEvaluationException e) {
      throw e;
    } catch (Exception e) {
      throw new ExpressionEvaluationException(e);
    }
  }

  public static Object evaluate(String formula, List<Arity> arity) throws ExpressionEvaluationException {
    if (StringUtils.isBlank(formula)) {
      throw new ExpressionEvaluationException("Empty formula.");
    }
    if (CollectionUtils.isEmpty(arity)) {
      throw new ExpressionEvaluationException("Empty arity.");
    }

    Expression expression = AviatorEvaluator.getCachedExpression(formula);
    if (expression == null) {
      expression = AviatorEvaluator.compile(formula, true);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("[EVALUATOR] - Create new cache expression - " + formula);
      }
    }

    List<String> variables = expression.getVariableNames();
    if (CollectionUtils.isEmpty(variables)) {
      throw new ExpressionEvaluationException("Compile expression error.");
    }
    if (arity.size() < variables.size()) {
      throw new ExpressionEvaluationException("Wrong arity number.");
    }

    Map<String, Object> parameters = new HashMap<String, Object>(variables.size());
    for (Arity a : arity) {
      parameters.put(a.getName(), a.getValue());
    }
    Object o;
    try {
      o = expression.execute(parameters);
    } catch (Exception e) {
      o = null;
    }

    if (o instanceof String) {
      return o;
    } else if (o instanceof Number) {
      Number result = (Number) o;
      double d = result.doubleValue();
      if (Double.isNaN(d)) {
        throw new ExpressionEvaluationException("Cannot evaluate indeterminate form value.");
      } else if (Double.isInfinite(d)) {
        throw new ExpressionEvaluationException("Cannot evaluate infinity value.");
      }
      d = Double.parseDouble(DECIMAL_FORMAT.format(d));
      long l = (long) d;
      if (l == d) {
        return l;
      } else {
        return d;
      }
    } else {
      throw new ExpressionEvaluationException("Cannot evaluate any value.");
    }
  }

  public static void main(String[] args) throws ExpressionEvaluationException {
    String formula = "(x*1)/(y*1)";
    List<Arity> arity = new ArrayList<Arity>();
    arity.add(new Arity("x", 0.0));
    arity.add(new Arity("y", 20.0));
    Object value = Evaluator.evaluate(formula, arity);
    System.out.println(value);

    arity = new ArrayList<Arity>();
    arity.add(new Arity("x", 50.0));
    arity.add(new Arity("y", 0.0));
    value = Evaluator.evaluate(formula, arity);
    System.out.println(value);

    formula = "x";
    arity = new ArrayList<Arity>();
    arity.add(new Arity("x", 11));
    value = Evaluator.evaluate(formula, arity);
    System.out.println(value);

    formula = "date_add(x, -2)";
    arity = new ArrayList<Arity>();
    arity.add(new Arity("x", "2013-05-23"));
    value = Evaluator.evaluate(formula, arity);
    System.out.println(value);

  }
}
