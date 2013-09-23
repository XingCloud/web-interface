package com.xingcloud.webinterface.utils;

import static com.xingcloud.mysql.PropType.sql_datetime;
import static com.xingcloud.mysql.UpdateFunc.once;
import static com.xingcloud.webinterface.enums.SegmentExprType.CONST;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.SEGMENT_TYPE;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.TOTAL_USER;

import com.google.common.base.Strings;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.util.JSON;
import com.xingcloud.mysql.PropOrig;
import com.xingcloud.mysql.PropType;
import com.xingcloud.mysql.UpdateFunc;
import com.xingcloud.mysql.UserProp;
import com.xingcloud.webinterface.enums.SegmentExprType;
import com.xingcloud.webinterface.exception.UserPropertyException;

import java.util.Map.Entry;

public class SegmentUtils {

  public static boolean immobile(String projectId, String segment) throws UserPropertyException {
    if (Strings.isNullOrEmpty(segment) || TOTAL_USER.equals(segment)) {
      return false;
    }
    BasicDBObject dbo = (BasicDBObject) JSON.parse(segment);

    String propName;
    String typeString;
    Object propVal;
    BasicDBList conditions;
    BasicDBObject condition;
    UserProp up;
    PropType pt;
    UpdateFunc uf;
    SegmentExprType tp;

    for (Entry<String, Object> entry : dbo.entrySet()) {
      propName = entry.getKey();
//      up = UserPropertiesInfoManager.getManager().getUserProp(projectId, propName);
      up = new UserProp("register_time", PropType.sql_datetime, UpdateFunc.once, PropOrig.sys, "", "");
      if (up == null) {
        continue;
      }
      uf = up.getPropFunc();
      pt = up.getPropType();
      if (!(once.equals(uf) && sql_datetime.equals(pt))) {
        return false;
      }

      // Add By Z J Wu @ 2013-04-24
      /*
      不加这个判断, 会出现类似写死一个注册时间, 但查询一个区间段的visit的情况
        [
          {
            "items": [
              {
                "name": "x",
                "event_key": "click.*.*.*.*.*",
                "count_method": "USER_NUM",
                "segment": "{\"register_time\":\"2013-04-13\"}"
              }
            ],
            "formula": "x*1",
            "id": "m6967s0",
            "end_time": "2013-04-23",
            "start_time": "2013-04-17",
            "interval": "DAY",
            "type": "COMMON",
            "project_id": "sof-dp"
          }
        ]
       */
      propVal = entry.getValue();
      if (!(propVal instanceof BasicDBList)) {
        return false;
      }
      conditions = (BasicDBList) propVal;
      for (Object o : conditions) {
        condition = (BasicDBObject) o;
        typeString = condition.getString(SEGMENT_TYPE);
        if (CONST.name().equals(typeString)) {
          return false;
        }
      }
//      System.out.println("-------------->" + conditions);
//      if (!conditions.containsField(HANDLER_KEYWORD) || !conditions.containsField(DATE_ADD_FUNCTION_NAME)) {
//        return false;
//      }
    }
    return true;
  }

}
