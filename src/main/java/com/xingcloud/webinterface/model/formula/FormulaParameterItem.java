package com.xingcloud.webinterface.model.formula;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.xingcloud.webinterface.utils.WebInterfaceCommonUtils.truncateStar;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.TOTAL_EVENT;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.TOTAL_USER;
import static org.apache.commons.lang.StringUtils.trimToNull;

import com.google.common.base.Strings;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import com.xingcloud.webinterface.annotation.JsonName;
import com.xingcloud.webinterface.enums.Function;
import com.xingcloud.webinterface.exception.NumberOfDayException;
import com.xingcloud.webinterface.exception.XParameterException;
import com.xingcloud.webinterface.model.Filter;
import com.xingcloud.webinterface.syncmetric.model.AbstractSync;
import org.apache.commons.lang3.StringUtils;

public abstract class FormulaParameterItem {

  @JsonName("name")
  @SerializedName("name")
  @Expose
  protected String name;

  @JsonName("event_key")
  @SerializedName("event_key")
  @Expose
  protected String event;

  @JsonName("segment")
  @SerializedName("segment")
  @Expose
  protected String segment;

  @JsonName("filter")
  @SerializedName("filter")
  protected Filter filter;

  @JsonName("count_method")
  protected Function function;

  @JsonName("number_of_day_origin")
  @SerializedName("number_of_day_origin")
  @Expose
  protected Integer coverRangeOrigin;

  @JsonName("number_of_day")
  @SerializedName("number_of_day")
  @Expose
  protected Integer coverRange;

  @JsonName("scale")
  @SerializedName("scale")
  @Expose
  protected String scale;

  public FormulaParameterItem() {
  }

  public FormulaParameterItem(String name, String event, String segment, Filter filter, Function function,
                              Integer coverRangeOrigin, Integer coverRange, String scale) {
    this.name = name;
    this.event = event;
    this.segment = segment;
    this.filter = filter;
    this.function = function;
    this.coverRangeOrigin = coverRangeOrigin;
    this.coverRange = coverRange;
    this.scale = scale;
  }

  public abstract boolean canAccumulateTotalAndNatural();

  public abstract AbstractSync makeSync(FormulaParameterContainer container);

  public void init(FormulaParameterContainer container) throws XParameterException {
    String event = trimToNull(getEvent());
    setEvent(event == null ? TOTAL_EVENT : truncateStar(event));

    String tmpSegment = trimToNull(getSegment());
    tmpSegment = isNullOrEmpty(tmpSegment) ? TOTAL_USER : tmpSegment;
    // 认为穿过来的json都合法
    // if (!TOTAL_USER.equals(tmpSegment)) {
    // Map<Object, Object> segmentMap = JsonUtils.json2Map(tmpSegment);
    // if (segmentMap == null || segmentMap.isEmpty()) {
    // tmpSegment = TOTAL_USER;
    // }
    // }
    setSegment(tmpSegment);

    Filter filter = getFilter();
    setFilter(filter == null ? Filter.ALL : filter);
    // setFilter(null);
  }

  public String getScale() {
    return scale;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getEvent() {
    return event;
  }

  public void setEvent(String event) {
    this.event = event;
  }

  public String getSegment() {
    return segment;
  }

  public void setSegment(String segment) {
    this.segment = segment;
  }

  public Filter getFilter() {
    return filter;
  }

  public void setFilter(Filter filter) {
    this.filter = filter;
  }

  public Function getFunction() {
    return function;
  }

  public void setFunction(Function function) {
    this.function = function;
  }

  public Integer getCoverRangeOrigin() {
    return coverRangeOrigin;
  }

  public void setCoverRangeOrigin(Integer coverRangeOrigin) {
    this.coverRangeOrigin = coverRangeOrigin;
  }

  public Integer getCoverRange() {
    return coverRange;
  }

  public void setCoverRange(Integer coverRange) {
    this.coverRange = coverRange;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((coverRange == null) ? 0 : coverRange.hashCode()
    );
    result = prime * result + ((coverRangeOrigin == null) ? 0 : coverRangeOrigin.hashCode()
    );
    result = prime * result + ((event == null) ? 0 : event.hashCode());
    result = prime * result + ((filter == null) ? 0 : filter.hashCode());
    result = prime * result + ((function == null) ? 0 : function.hashCode());
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result + ((segment == null) ? 0 : segment.hashCode());
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
    FormulaParameterItem other = (FormulaParameterItem) obj;
    if (coverRange == null) {
      if (other.coverRange != null)
        return false;
    } else if (!coverRange.equals(other.coverRange))
      return false;
    if (coverRangeOrigin == null) {
      if (other.coverRangeOrigin != null)
        return false;
    } else if (!coverRangeOrigin.equals(other.coverRangeOrigin))
      return false;
    if (event == null) {
      if (other.event != null)
        return false;
    } else if (!event.equals(other.event))
      return false;
    if (filter == null) {
      if (other.filter != null)
        return false;
    } else if (!filter.equals(other.filter))
      return false;
    if (function != other.function)
      return false;
    if (name == null) {
      if (other.name != null)
        return false;
    } else if (!name.equals(other.name))
      return false;
    if (segment == null) {
      if (other.segment != null)
        return false;
    } else if (!segment.equals(other.segment))
      return false;
    return true;
  }

  public void validate(FormulaParameterContainer fpc) throws XParameterException, NumberOfDayException {
    if (Strings.isNullOrEmpty(getName())) {
      throw new XParameterException();
    }
    if (getFunction() == null) {
      throw new XParameterException();
    }
    if (StringUtils.isBlank(scale)) {
      throw new XParameterException("Scale is null");
    }
  }

}
