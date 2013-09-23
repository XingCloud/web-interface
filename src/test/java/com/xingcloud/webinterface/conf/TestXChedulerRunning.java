package com.xingcloud.webinterface.conf;

import com.xingcloud.webinterface.cron.XScheduler;
import org.junit.Test;

/**
 * User: Z J Wu Date: 13-9-5 Time: 下午5:06 Package: com.xingcloud.webinterface.conf
 */
public class TestXChedulerRunning {

  @Test
  public void test() {
    XScheduler xScheduler = XScheduler.getInstance();
  }
}
