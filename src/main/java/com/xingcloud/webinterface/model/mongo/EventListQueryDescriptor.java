package com.xingcloud.webinterface.model.mongo;

import com.google.common.base.Strings;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import com.xingcloud.webinterface.utils.JsonUtils;
import com.xingcloud.webinterface.utils.WebInterfaceConstants;
import net.sf.json.JSONObject;

import java.util.Iterator;
import java.util.Map;

public class EventListQueryDescriptor {

  public static EventListQueryDescriptor toEventListQueryDescriptor(String json) throws InstantiationException,
    IllegalAccessException {
    if (Strings.isNullOrEmpty(json)) {
      return null;
    }
    EventListQueryDescriptor eqd = new EventListQueryDescriptor();
    JSONObject jo = JSONObject.fromObject(json);
    String projectId = (String) jo.get("project_id");
    String targetRow = (String) jo.get("target_row");
    Map<Object, Object> m = JsonUtils.json2Map(jo.get("condition"));
    eqd.setProjectId(projectId);
    eqd.setTargetRow(targetRow);
    eqd.setCondition(m);
    return eqd;
  }

  @Expose
  @SerializedName(WebInterfaceConstants.TBL_USER_PROPERTIES_PROJECT_ID)
  private String projectId;

  @Expose
  @SerializedName("target_row")
  private String targetRow;

  @Expose
  @SerializedName("condition")
  private Map<Object, Object> condition;

  public EventListQueryDescriptor() {
    super();
  }

  public EventListQueryDescriptor(String projectId, String targetRow, Map<Object, Object> condition) {
    super();
    this.projectId = projectId;
    this.targetRow = targetRow;
    this.condition = condition;
  }

  public String getProjectId() {
    return projectId;
  }

  public void setProjectId(String projectId) {
    this.projectId = projectId;
  }

  public String getTargetRow() {
    return targetRow;
  }

  public void setTargetRow(String targetRow) {
    this.targetRow = targetRow;
  }

  public Map<Object, Object> getCondition() {
    return condition;
  }

  public void setCondition(Map<Object, Object> condition) {
    this.condition = condition;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("EQD.");
    sb.append(projectId);
    sb.append('.');
    sb.append(targetRow);
    sb.append(".{");
    if (condition != null) {
      Object k = null;
      Object v = null;
      Iterator<Object> it = condition.keySet().iterator();
      if (it.hasNext()) {
        for (; ; ) {
          k = it.next();
          sb.append(k.toString());
          sb.append('(');
          sb.append(k.getClass());
          sb.append("): ");
          v = condition.get(k);
          if (v != null) {
            sb.append(v);
            sb.append('(');
            sb.append(v.getClass());
            sb.append(')');
          } else {
            sb.append("null");
          }
          if (it.hasNext()) {
            sb.append(", ");
          } else {
            break;
          }
        }
      }
    }
    sb.append('}');
    return sb.toString();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((condition == null) ? 0 : condition.hashCode());
    result = prime * result + ((projectId == null) ? 0 : projectId.hashCode());
    result = prime * result + ((targetRow == null) ? 0 : targetRow.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    EventListQueryDescriptor other = (EventListQueryDescriptor) obj;
    if (condition == null) {
      if (other.condition != null)
        return false;
    } else if (!condition.equals(other.condition))
      return false;
    if (projectId == null) {
      if (other.projectId != null)
        return false;
    } else if (!projectId.equals(other.projectId))
      return false;
    if (targetRow == null) {
      if (other.targetRow != null)
        return false;
    } else if (!targetRow.equals(other.targetRow))
      return false;
    return true;
  }

  public static void main(String[] args) throws InstantiationException, IllegalAccessException {
    String json = "{\"project_id\": \"ranchfacebook\", \"target_row\": \"l3\", " + "\"condition\":{\"l1\": \"buy\", \"l2\": \"dog\"}}";
    String json2 = "{\"project_id\": \"ranchfacebook\", \"target_row\": \"l3\"}";
    EventListQueryDescriptor eqd = EventListQueryDescriptor.toEventListQueryDescriptor(json2);
    System.out.println(eqd);
  }

}
