package com.xingcloud.webinterface.conf;

import com.xingcloud.webinterface.remote.WebServiceProvider;
import org.junit.Test;

/**
 * User: Z J Wu Date: 13-9-6 Time: 下午3:30 Package: com.xingcloud.webinterface.conf
 */
public class TestLoadWebService {

  @Test
  public void test() {
    Object service = WebServiceProvider.provideService("QUERY-LP");
    System.out.println(service);
  }
}
