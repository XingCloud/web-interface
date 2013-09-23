package com.xingcloud.webinterface.utils;

import net.sf.json.JSONObject;

import java.util.HashMap;
import java.util.Map;

public class JsonUtils {

  public static Map<Object, Object> json2Map(Object jsonString) {
    if (jsonString == null) {
      return null;
    }
    JSONObject jsonObject = JSONObject.fromObject(jsonString);
    Map<Object, Object> result = new HashMap<Object, Object>();

    for (Object obj : jsonObject.keySet()) {
      if (jsonObject.get(obj) instanceof JSONObject) {
        result.put(obj, json2Map(jsonObject.get(obj)));
      } else {
        result.put(obj, jsonObject.get(obj));
      }
    }
    return result;
  }

  public static void main(String[] args) {
    String url2 = "{\"first_pay_amount\":{\"$ge\": 1000},\"first_pay_time\":" + "\"2012-04-10\",\"last_login_time\":{\"$handler\": \"DateSplittor\"}}";
    Map<Object, Object> map = json2Map(url2);
    System.out.println(map);
  }

}
