package com.xingcloud.webinterface.plan;

import static com.xingcloud.meta.ByteUtils.toBytes;

import org.junit.Test;

import java.io.UnsupportedEncodingException;

/**
 * User: Z J Wu Date: 13-8-14 Time: 下午3:10 Package: com.xingcloud.webinterface.plan
 */
public class TestRowkeyRange {

  @Test
  public void testRowkeyRange() throws UnsupportedEncodingException {
    String startKey = "201308014a";
    String endKey = "201308014中文";
    RowkeyRange rr = RowkeyRange.create(toBytes(startKey), toBytes(endKey));
    System.out.println(rr);
  }
}
