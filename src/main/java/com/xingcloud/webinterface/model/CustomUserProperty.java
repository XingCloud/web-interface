package com.xingcloud.webinterface.model;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import com.xingcloud.mysql.PropType;
import com.xingcloud.mysql.UpdateFunc;
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
  @Expose
  @SerializedName("func")
  private UpdateFunc func;

  public CustomUserProperty() {
    super();
  }

  public CustomUserProperty(String projectId, String name, String nickname, PropType type, String slicePattern,
                            UpdateFunc func) {
    this.projectId = projectId;
    this.name = name;
    this.nickname = nickname;
    this.type = type;
    this.slicePattern = slicePattern;
    this.func = func;
  }

  public UpdateFunc getFunc() {
    return func;
  }

  public void setFunc(UpdateFunc func) {
    this.func = func;
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
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CustomUserProperty)) {
      return false;
    }

    CustomUserProperty that = (CustomUserProperty) o;

    if (func != that.func) {
      return false;
    }
    if (name != null ? !name.equals(that.name) : that.name != null) {
      return false;
    }
    if (nickname != null ? !nickname.equals(that.nickname) : that.nickname != null) {
      return false;
    }
    if (projectId != null ? !projectId.equals(that.projectId) : that.projectId != null) {
      return false;
    }
    if (slicePattern != null ? !slicePattern.equals(that.slicePattern) : that.slicePattern != null) {
      return false;
    }
    if (type != that.type) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = projectId != null ? projectId.hashCode() : 0;
    result = 31 * result + (name != null ? name.hashCode() : 0);
    result = 31 * result + (nickname != null ? nickname.hashCode() : 0);
    result = 31 * result + (type != null ? type.hashCode() : 0);
    result = 31 * result + (slicePattern != null ? slicePattern.hashCode() : 0);
    result = 31 * result + (func != null ? func.hashCode() : 0);
    return result;
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
