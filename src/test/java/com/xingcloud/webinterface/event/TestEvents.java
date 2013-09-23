package com.xingcloud.webinterface.event;

import com.xingcloud.events.XEvent;
import com.xingcloud.events.XEventException;
import com.xingcloud.events.XEventOperation;
import com.xingcloud.memcache.MemCacheException;
import org.junit.Test;

import java.util.List;

/**
 * User: Z J Wu Date: 13-9-10 Time: 下午3:01 Package: com.xingcloud.webinterface.event
 */
public class TestEvents {

  @Test
  public void test() throws XEventException, MemCacheException {
    XEventOperation operation = XEventOperation.getInstance();
    List<XEvent> xEventList;
    xEventList = operation.getEvents("sof-dsk", "visit.*", true);
    System.out.println("---------------------------------");
    for (XEvent event : xEventList) {
      System.out.println(event.nameRowkeyStyle());
    }
  }
}
