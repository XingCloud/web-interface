package com.xingcloud.webinterface.up;

import com.xingcloud.mysql.UserProp;
import com.xingcloud.webinterface.exception.UserPropertyException;
import com.xingcloud.webinterface.utils.UserPropertiesInfoManager;
import org.junit.Test;

/**
 * User: Z J Wu Date: 13-8-29 Time: 上午11:02 Package: com.xingcloud.webinterface.up
 */
public class TestUserProperties {

  @Test
  public void testUP() throws UserPropertyException {
    UserPropertiesInfoManager userPropertiesInfoManager = UserPropertiesInfoManager.getInstance();
    UserProp up = userPropertiesInfoManager.getUserProp("age", "register_time");
    System.out.println(up);
    up = userPropertiesInfoManager.getUserProp("age", "register_time");
    up = userPropertiesInfoManager.getUserProp("age", "first_pay_time");
  }
}
