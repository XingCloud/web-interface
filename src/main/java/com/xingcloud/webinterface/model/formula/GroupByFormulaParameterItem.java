package com.xingcloud.webinterface.model.formula;

import com.google.common.base.Strings;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import com.xingcloud.webinterface.annotation.JsonName;
import com.xingcloud.webinterface.enums.Function;
import com.xingcloud.webinterface.enums.GroupByType;
import com.xingcloud.webinterface.exception.NumberOfDayException;
import com.xingcloud.webinterface.exception.XParameterException;
import com.xingcloud.webinterface.model.Filter;
import com.xingcloud.webinterface.syncmetric.model.AbstractSync;
import com.xingcloud.webinterface.syncmetric.model.GroupBySync;

public class GroupByFormulaParameterItem extends FormulaParameterItem {

  @JsonName("groupby")
  @SerializedName("groupby")
  @Expose
  private String groupBy;

  @JsonName("groupby_type")
  @SerializedName("groupby_type")
  @Expose
  private GroupByType groupByType;

  // 长度, 用于离线计算 离线计算不传输日期, 因此需要指定长度
  @JsonName("length")
  private Integer length;

  @JsonName("groupby_json")
  private String groupByJson;

  public GroupByFormulaParameterItem() {
  }

  public GroupByFormulaParameterItem(String name, String event, String segment, Filter filter, Function function,
                                     Integer coverRangeOrigin, Integer coverRange, String groupBy,
                                     GroupByType groupByType, String scale) {
    super(name, event, segment, filter, function, coverRangeOrigin, coverRange, scale);
    this.groupBy = groupBy;
    this.groupByType = groupByType;
  }

  public GroupByFormulaParameterItem(String name, String event, String segment, Filter filter, Function function,
                                     Integer coverRangeOrigin, Integer coverRange, String groupBy,
                                     GroupByType groupByType, Integer length, String groupByJson, String scale) {
    super(name, event, segment, filter, function, coverRangeOrigin, coverRange, scale);
    this.groupBy = groupBy;
    this.groupByType = groupByType;
    this.length = length;
    this.groupByJson = groupByJson;
  }

  public void init(FormulaParameterContainer container) throws XParameterException {
    super.init(container);
    if (this.groupByType == null) {
      this.groupByType = GroupByType.USER_PROPERTIES;
    }
  }

  public String getGroupBy() {
    return groupBy;
  }

  public void setGroupBy(String groupBy) {
    this.groupBy = groupBy;
  }

  public GroupByType getGroupByType() {
    return groupByType;
  }

  public void setGroupByType(GroupByType groupByType) {
    this.groupByType = groupByType;
  }

  public Integer getLength() {
    return length;
  }

  public void setLength(Integer length) {
    this.length = length;
  }

  public String getGroupByJson() {
    return groupByJson;
  }

  public void setGroupByJson(String groupByJson) {
    this.groupByJson = groupByJson;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("GBPI(");
    sb.append(name);
    sb.append('.');
    sb.append(event);
    sb.append('.');
    sb.append(segment);
    sb.append('.');
    sb.append(filter);
    sb.append('.');
    sb.append(function);
    sb.append('.');
    sb.append(this.groupBy);
    sb.append('.');
    sb.append(this.groupByType);
    sb.append(".(CR.");
    sb.append(this.coverRange);
    sb.append(".CRO.");
    sb.append(this.coverRangeOrigin);
    sb.append(")#");
    sb.append(this.length);
    return sb.toString();
  }

  @Override
  public void validate(FormulaParameterContainer fpc) throws XParameterException, NumberOfDayException {
    super.validate(fpc);
    if (this.groupByType == null) {
      throw new XParameterException("Group by type cannot be null.");
    }
    if ((!this.groupByType.equals(GroupByType.EVENT_VAL)) && Strings.isNullOrEmpty(groupBy)) {
      throw new XParameterException("Group by string cannot be null or empty.");
    }
  }

  @Override
  public boolean canAccumulateTotalAndNatural() {
    return false;
  }

  @Override
  public AbstractSync makeSync(FormulaParameterContainer container) {
    String projectId = container.getProjectId();
    String event = getEvent();
    String segment = getSegment();
    Integer coverRangeOrigin = getCoverRangeOrigin();
    Integer coverRange = getCoverRange();
    Integer length = getLength();
    String groupByJson = getGroupByJson();
    AbstractSync sync = new GroupBySync(projectId, event, segment, coverRangeOrigin, coverRange, length, groupByJson);
    return sync;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((groupBy == null) ? 0 : groupBy.hashCode());
    result = prime * result + ((groupByJson == null) ? 0 : groupByJson.hashCode()
    );
    result = prime * result + ((groupByType == null) ? 0 : groupByType.hashCode()
    );
    result = prime * result + ((length == null) ? 0 : length.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    GroupByFormulaParameterItem other = (GroupByFormulaParameterItem) obj;
    if (groupBy == null) {
      if (other.groupBy != null)
        return false;
    } else if (!groupBy.equals(other.groupBy))
      return false;
    if (groupByJson == null) {
      if (other.groupByJson != null)
        return false;
    } else if (!groupByJson.equals(other.groupByJson))
      return false;
    if (groupByType != other.groupByType)
      return false;
    if (length == null) {
      if (other.length != null)
        return false;
    } else if (!length.equals(other.length))
      return false;
    return true;
  }

}
