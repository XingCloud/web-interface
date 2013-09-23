package com.xingcloud.webinterface.calculate.func;

import static com.xingcloud.basic.utils.DateUtils.dateAdd;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.SEGMENT_FUNCTION_BEGIN_CHAR;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.runtime.function.AbstractFunction;
import com.googlecode.aviator.runtime.function.FunctionUtils;
import com.googlecode.aviator.runtime.type.AviatorObject;
import com.googlecode.aviator.runtime.type.AviatorString;

import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

/**
 * User: Z J Wu Date: 13-5-23 Time: 下午3:14 Package: com.xingcloud.webinterface.calculate.func
 */
public class DateAddFunction extends AbstractFunction {

  public static final String DATE_ADD_FUNCTION_NAME = SEGMENT_FUNCTION_BEGIN_CHAR + "date_add";

  @Override
  public String getName() {
    return DATE_ADD_FUNCTION_NAME;
  }

  public AviatorObject call(Map<String, Object> env, AviatorObject operationDate, AviatorObject offsetValue) {
    String date = FunctionUtils.getStringValue(operationDate, env);
    int offset = FunctionUtils.getNumberValue(offsetValue, env).intValue();
    try {
      return new AviatorString(dateAdd(date, -offset));
    } catch (ParseException e) {
    }
    return null;
  }

  public static void main(String[] args) {
    String expr = "$date_add(x, 2)";
    AviatorEvaluator.addFunction(new DateAddFunction());
    Map<String, Object> env = new HashMap<String, Object>(1);
    env.put("x", "2013-05-20");
    System.out.println(AviatorEvaluator.execute(expr, env));
  }
}
