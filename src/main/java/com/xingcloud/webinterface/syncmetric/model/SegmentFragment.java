package com.xingcloud.webinterface.syncmetric.model;

import com.google.common.base.Strings;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import com.xingcloud.webinterface.utils.WebInterfaceConstants;

import java.util.Map;

public class SegmentFragment {

  @Expose
  @SerializedName("project_id")
  private String projectId;

  @Expose
  @SerializedName("segments")
  private Map<String, String> segmentFragmentsMap;

  public SegmentFragment() {
  }

  public SegmentFragment(String projectId, Map<String, String> segmentFragmentsMap) {
    this.projectId = projectId;
    this.segmentFragmentsMap = segmentFragmentsMap;
  }

  public String getProjectId() {
    return projectId;
  }

  public void setProjectId(String projectId) {
    this.projectId = projectId;
  }

  public Map<String, String> getSegmentFragmentsMap() {
    return segmentFragmentsMap;
  }

  public void setSegmentFragmentsMap(Map<String, String> segmentFragmentsMap) {
    this.segmentFragmentsMap = segmentFragmentsMap;
  }

  @Override
  public String toString() {
    return "SF." + projectId + "." + segmentFragmentsMap;
  }

  public void trim() {
    if (Strings.isNullOrEmpty(getProjectId())) {
      setProjectId(WebInterfaceConstants.DEFAULT_PROJECT_ID_IDENTIFIER);
    }
  }

}
