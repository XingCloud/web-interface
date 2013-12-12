package com.xingcloud.webinterface;

import com.xingcloud.memcache.MemCacheException;
import com.xingcloud.webinterface.calculate.FormulaGroup;
import com.xingcloud.webinterface.exception.FormulaException;
import org.junit.Test;

/**
 * User: Z J Wu Date: 13-12-12 Time: 下午4:34 Package: com.xingcloud.webinterface
 */
public class TestFormula {

  @Test
  public void test() throws MemCacheException, FormulaException {
    String formula = "x*1|2013-10-01:x*2|2013-11-01:x*3|2013-12-01:x*4";
    formula = "x*1|2013-10-01:x*2";
    FormulaGroup fg = FormulaGroup.buildFormulaGroup(formula);
    String f = fg.getFormula("2013-09-30");
    fg = FormulaGroup.buildFormulaGroup(formula);
    f = fg.getFormula("2013-10-30");
    System.out.println(f);
  }
}
