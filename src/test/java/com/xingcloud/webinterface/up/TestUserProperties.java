package com.xingcloud.webinterface.up;

import com.xingcloud.mysql.MySql_16seqid;
import com.xingcloud.mysql.UserProp;
import com.xingcloud.webinterface.exception.UserPropertyException;
import org.junit.Test;

import java.sql.SQLException;
import java.util.List;

/**
 * User: Z J Wu Date: 13-8-29 Time: 上午11:02 Package: com.xingcloud.webinterface.up
 */
public class TestUserProperties {

  @Test
  public void testUP() throws UserPropertyException, SQLException {
    MySql_16seqid manager = MySql_16seqid.getInstance();
    List<UserProp> result = manager.getUserProps("thevideomate");
    for (UserProp up : result) {
      System.out.println(up.getPropName() + "|" + up.getPropAlias());
    }
  }

}
