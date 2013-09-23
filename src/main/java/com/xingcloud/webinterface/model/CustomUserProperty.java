package com.xingcloud.webinterface.model;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import com.xingcloud.mysql.PropType;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;

public class CustomUserProperty implements Serializable {
  private static final long serialVersionUID = -5769426163027139901L;
  @Expose
  @SerializedName("project_id")
  private String projectId;
  @Expose
  @SerializedName("name")
  private String name;
  @Expose
  @SerializedName("nickname")
  private String nickname;
  @Expose
  @SerializedName("type")
  private PropType type;
  @Expose
  @SerializedName("groupby_pattern")
  private String slicePattern;

  public CustomUserProperty() {
    super();
  }

  public CustomUserProperty(String projectId, String name, String nickname, PropType type, String slicePattern) {
    this.projectId = projectId;
    this.name = name;
    this.nickname = nickname;
    this.type = type;
    this.slicePattern = slicePattern;
  }

  public String getProjectId() {
    return projectId;
  }

  public void setProjectId(String projectId) {
    this.projectId = projectId;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getNickname() {
    return nickname;
  }

  public void setNickname(String nickname) {
    this.nickname = nickname;
  }

  public PropType getType() {
    return type;
  }

  public void setType(PropType type) {
    this.type = type;
  }

  public String getSlicePattern() {
    return slicePattern;
  }

  public void setSlicePattern(String slicePattern) {
    this.slicePattern = slicePattern;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result + ((nickname == null) ? 0 : nickname.hashCode());
    result = prime * result + ((projectId == null) ? 0 : projectId.hashCode());
    result = prime * result + ((slicePattern == null) ? 0 : slicePattern.hashCode()
    );
    result = prime * result + ((type == null) ? 0 : type.hashCode());
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
    CustomUserProperty other = (CustomUserProperty) obj;
    if (name == null) {
      if (other.name != null)
        return false;
    } else if (!name.equals(other.name))
      return false;
    if (nickname == null) {
      if (other.nickname != null)
        return false;
    } else if (!nickname.equals(other.nickname))
      return false;
    if (projectId == null) {
      if (other.projectId != null)
        return false;
    } else if (!projectId.equals(other.projectId))
      return false;
    if (slicePattern == null) {
      if (other.slicePattern != null)
        return false;
    } else if (!slicePattern.equals(other.slicePattern))
      return false;
    if (type != other.type)
      return false;
    return true;
  }

  public String toString() {
    return "CustomUserProperty." + projectId + "." + name + "(" + nickname + ")." + type + "." + slicePattern;
  }

  public boolean validateAndTrim() {
    String s = StringUtils.trimToNull(this.projectId);
    if (StringUtils.isBlank(s)) {
      return false;
    }
    this.projectId = s;
    s = StringUtils.trimToNull(this.name);
    if (StringUtils.isBlank(s)) {
      return false;
    }
    this.name = s;
    if (type == null) {
      return false;
    }

    if (StringUtils.isBlank(this.nickname)) {
      this.nickname = this.name;
    } else {
      this.nickname = StringUtils.trimToNull(this.nickname);
    }
    if (StringUtils.isNotBlank(this.slicePattern)) {
      this.slicePattern = StringUtils.trimToEmpty(this.slicePattern);
      this.slicePattern = this.slicePattern.replace(" ", "");
    }
    return true;
  }

}
