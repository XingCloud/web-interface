package com.xingcloud.webinterface.conf;

import org.apache.commons.configuration.Configuration;
import org.junit.Test;

/**
 * User: Z J Wu Date: 13-9-5 Time: 下午3:27 Package: com.xingcloud.webinterface.conf
 */
public class TestConfigLoad {

  @Test
  public void testConfig() {
    Configuration configuration=WebInterfaceConfig.getConfiguration();
  }
}
