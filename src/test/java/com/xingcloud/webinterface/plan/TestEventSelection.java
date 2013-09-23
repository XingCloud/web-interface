package com.xingcloud.webinterface.plan;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xingcloud.webinterface.exception.PlanException;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.junit.Test;

import java.io.UnsupportedEncodingException;

/**
 * User: Z J Wu Date: 13-8-14 Time: 下午3:10 Package: com.xingcloud.webinterface.plan
 */
public class TestEventSelection {

  @Test
  public void testRowkeyRange() throws UnsupportedEncodingException, PlanException, JsonProcessingException {
    String projectId = "age";
    String event = "pay.*";
    LogicalOperator operator = Plans.getEventScan(projectId, event, "2013-08-01", "2013-08-01", "ts");
    ObjectMapper mapper = Plans.DEFAULT_DRILL_CONFIG.getMapper();
    System.out.println(mapper.writeValueAsString(operator));
  }
}
