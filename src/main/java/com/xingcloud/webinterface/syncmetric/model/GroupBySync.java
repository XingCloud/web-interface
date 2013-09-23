package com.xingcloud.webinterface.syncmetric.model;

import com.google.common.base.Strings;
import com.xingcloud.webinterface.annotation.Ignore;
import com.xingcloud.webinterface.enums.GroupByType;
import com.xingcloud.webinterface.exception.RefelectException;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GroupBySync extends AbstractSync {

  @Ignore
  private String groupByJson;

  public GroupBySync() {
    super();
  }

  public GroupBySync(String projectId, String event, String segment, Integer coverRangeOrigin, Integer coverRange,
                     Integer length, String groupByJson) {
    super(projectId, event, segment, coverRangeOrigin, coverRange, length);
    this.groupByJson = groupByJson;
  }

  public String getGroupByJson() {
    return groupByJson;
  }

  public void setGroupByJson(String groupByJson) {
    this.groupByJson = groupByJson;
  }

  public Map<GroupByType, List<Object>> getGroupByMap() {
    if (Strings.isNullOrEmpty(groupByJson)) {
      return null;
    }
    Map<GroupByType, List<Object>> groupByMap = new HashMap<GroupByType, List<Object>>();
    JSONObject jo = JSONObject.fromObject(groupByJson);
    List<Object> list = null;
    JSONArray ja = null;
    GroupByType gbt = null;
    for (Object key : jo.keySet()) {
      gbt = Enum.valueOf(GroupByType.class, key.toString());
      if (!groupByMap.containsKey(key)) {
        groupByMap.put(gbt, new ArrayList<Object>());
      }
      list = groupByMap.get(gbt);
      ja = (JSONArray) jo.get(key);
      for (Object v : ja) {
        list.add(v);
      }
    }
    return groupByMap;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("GroupBySync.");
    sb.append(projectId);
    sb.append('.');
    sb.append(event);
    sb.append('.');
    sb.append(segment);
    sb.append('.');
    sb.append(groupByJson);
    sb.append("(CR.");
    sb.append(coverRange);
    sb.append(".CRO.");
    sb.append(coverRangeOrigin);
    sb.append(")#");
    sb.append(length);
    return sb.toString();
  }

  public static void main(String[] args) throws RefelectException {
    String s = "{\"EVENT\":[\"a\",\"b\"], \"USER_PROPERTIES\":[\"grade\"]}";
    GroupBySync gbs = new GroupBySync();
    gbs.setGroupByJson(s);

    System.out.println(gbs.toMap());
  }

}
