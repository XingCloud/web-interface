package com.xingcloud.webinterface.up;

import com.xingcloud.memcache.MemCacheException;
import com.xingcloud.webinterface.calculate.ScaleGroup;
import com.xingcloud.webinterface.exception.FormulaException;
import org.junit.Test;

/**
 * User: Z J Wu Date: 13-12-16 Time: 下午3:11 Package: com.xingcloud.webinterface.up
 */
public class TestScale {

  @Test
  public void getScale() throws MemCacheException, FormulaException {
    String scale = "1970-01-01:1|2013-10-01:0.2|2013-11-01:0.3|2013-12-01:0.4";
//    scale = "1970-01-01:0.5|2013-10-01:0.2";
    ScaleGroup scaleGroup = ScaleGroup.buildScaleGroup(scale);
    System.out.println(scaleGroup.getScale("2013-12-16"));
    scaleGroup = ScaleGroup.buildScaleGroup(scale);
    System.out.println(scaleGroup.getScale("2013-09-30"));
    scaleGroup = ScaleGroup.buildScaleGroup(scale);
    System.out.println(scaleGroup.getScale("2013-10-01"));
  }
}
