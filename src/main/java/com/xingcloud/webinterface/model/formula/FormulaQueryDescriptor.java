package com.xingcloud.webinterface.model.formula;

import static com.xingcloud.basic.Constants.SEPARATOR_CHAR_CACHE;
import static com.xingcloud.basic.Constants.SEPARATOR_CHAR_TOSTRING;
import static com.xingcloud.basic.utils.DateUtils.span;
import static com.xingcloud.webinterface.enums.DateTruncateType.KILL;
import static com.xingcloud.webinterface.enums.Function.COUNT;
import static com.xingcloud.webinterface.enums.Function.SUM;
import static com.xingcloud.webinterface.enums.Function.USER_NUM;
import static com.xingcloud.webinterface.enums.QueryType.COMMON;
import static com.xingcloud.webinterface.enums.QueryType.GROUP;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.TOTAL_EVENT;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.TOTAL_USER;

import com.google.common.base.Strings;
import com.xingcloud.mysql.UserProp;
import com.xingcloud.webinterface.annotation.JsonName;
import com.xingcloud.webinterface.enums.DateTruncateType;
import com.xingcloud.webinterface.enums.Function;
import com.xingcloud.webinterface.exception.PlanException;
import com.xingcloud.webinterface.model.Filter;
import com.xingcloud.webinterface.segment.XSegment;
import org.apache.commons.collections.CollectionUtils;
import org.apache.drill.common.logical.LogicalPlan;

import java.lang.reflect.Field;
import java.text.ParseException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Z J Wu@2012-11-27
 */
public abstract class FormulaQueryDescriptor {
  public static final Map<String, String> JSON_NAME_FIELD_NAME_MAP = new HashMap<String, String>();

  static {
    Class clz = FormulaQueryDescriptor.class;
    Field[] fields = clz.getDeclaredFields();
    JsonName jsonNameAnnotation;
    for (Field field : fields) {
      jsonNameAnnotation = field.getAnnotation(JsonName.class);
      if (jsonNameAnnotation == null) {
        continue;
      }
      JSON_NAME_FIELD_NAME_MAP.put(jsonNameAnnotation.value(), field.getName());
    }
  }

  // Cache-Key组成部分
  protected String projectId;
  protected String realBeginDate;
  protected String realEndDate;
  protected String event;
  // Segment相关
  protected String sqlSegments;
  protected XSegment segment;

  protected Filter filter;

  // 其他信息
  protected Set<Function> functions = EnumSet.noneOf(Function.class);
  protected long dayDistance;
  protected DateTruncateType dateTruncateType;
  protected String key;
  protected List<UserProp> userProperties;
  protected boolean incremental;

  // 给segment使用的开始时间
  @JsonName("start_date")
  protected String inputBeginDate;
  // 给segment使用的结束时间
  @JsonName("end_date")
  protected String inputEndDate;

  public FormulaQueryDescriptor() {
    super();
  }

  public FormulaQueryDescriptor(String projectId, String realBeginDate, String realEndDate, String event,
                                String sqlSegments, Filter filter) {
    super();
    this.projectId = projectId;
    this.realBeginDate = realBeginDate;
    this.realEndDate = realEndDate;
    this.event = event;
    this.sqlSegments = sqlSegments;
    this.filter = filter;
    try {
      setDayDistance(span(realBeginDate, realEndDate));
    } catch (ParseException e) {
      e.printStackTrace();
    }
    this.inputBeginDate = realBeginDate;
    this.inputEndDate = realEndDate;
  }

  public FormulaQueryDescriptor(String projectId, String realBeginDate, String realEndDate, String event,
                                String sqlSegments, Filter filter, String inputBeginDate, String inputEndDate) {
    super();
    this.projectId = projectId;
    this.realBeginDate = realBeginDate;
    this.realEndDate = realEndDate;
    this.event = event;
    this.sqlSegments = sqlSegments;
    this.filter = filter;
    try {
      setDayDistance(span(realBeginDate, realEndDate));
    } catch (ParseException e) {
      e.printStackTrace();
    }
    this.inputBeginDate = inputBeginDate;
    this.inputEndDate = inputEndDate;
  }

  public boolean hasSegment() {
    String segment = getSqlSegments();
    return !(Strings.isNullOrEmpty(segment) || TOTAL_USER.equals(segment)
    );
  }

  public boolean isCommon() {
    return this instanceof CommonFormulaQueryDescriptor;
  }

  public boolean isGroupBy() {
    return this instanceof GroupByFormulaQueryDescriptor;
  }

  public boolean isKilled() {
    return KILL.equals(getDateTruncateType());
  }

  public boolean isIncremental() {
    return incremental;
  }

  public void setIncremental(boolean incremental) {
    this.incremental = incremental;
  }

  public abstract LogicalPlan toLogicalPlain() throws PlanException;

  protected void toStringGeneric(StringBuilder sb) {
    if (sb == null) {
      return;
    }
    sb.append(SEPARATOR_CHAR_TOSTRING);
    sb.append("IB=");
    sb.append(getInputBeginDate());
    sb.append(SEPARATOR_CHAR_CACHE);
    sb.append("IE=");
    sb.append(getInputEndDate());
    sb.append(SEPARATOR_CHAR_CACHE);
    sb.append(getFunctions());
    sb.append(SEPARATOR_CHAR_CACHE);
    sb.append(getDateTruncateType());
  }

  protected StringBuilder toKeyGeneric() {
    StringBuilder sb = new StringBuilder();
    if (this instanceof CommonFormulaQueryDescriptor) {
      sb.append(COMMON.name());
    } else if (this instanceof GroupByFormulaQueryDescriptor) {
      sb.append(GROUP.name());
    } else {
      return null;
    }
    sb.append(SEPARATOR_CHAR_CACHE);

    sb.append(getProjectId());
    sb.append(SEPARATOR_CHAR_CACHE);

    sb.append(getRealBeginDate());
    sb.append(SEPARATOR_CHAR_CACHE);

    sb.append(getRealEndDate());
    sb.append(SEPARATOR_CHAR_CACHE);

    String event = getEvent();
    if (Strings.isNullOrEmpty(event)) {
      sb.append(TOTAL_EVENT);
    } else {
      sb.append(event);
    }
    sb.append(SEPARATOR_CHAR_CACHE);
    XSegment xSegment = getSegment();
    sb.append(xSegment == null ? TOTAL_USER : xSegment.getIdentifier().replace("'", ""));
    sb.append(SEPARATOR_CHAR_CACHE);

    Filter filter = getFilter();
    if (filter != null) {
      sb.append(filter);
    }
    return sb;
  }

  protected abstract String toKey();

  protected abstract String toKey(boolean ignore);

  public boolean isNeedCountFunction() {
    return containsFunction(COUNT);
  }

  public boolean isNeedSumFunction() {
    return containsFunction(SUM);
  }

  public boolean isNeedUserNumFunction() {
    return containsFunction(USER_NUM);
  }

  public boolean containsFunction(Function function) {
    return function != null && this.functions.contains(function);
  }

  public boolean addFunction(Function function) {
    return function != null && getFunctions().add(function);
  }

  public boolean addFunctions(Set<Function> functions) {
    return CollectionUtils.isNotEmpty(functions) && getFunctions().addAll(functions);
  }

  public String getProjectId() {
    return projectId;
  }

  public void setProjectId(String projectId) {
    this.projectId = projectId;
  }

  public String getRealBeginDate() {
    return realBeginDate;
  }

  public void setRealBeginDate(String realBeginDate) {
    this.realBeginDate = realBeginDate;
  }

  public String getRealEndDate() {
    return realEndDate;
  }

  public void setRealEndDate(String realEndDate) {
    this.realEndDate = realEndDate;
  }

  public String getEvent() {
    return event;
  }

  public void setEvent(String event) {
    this.event = event;
  }

  public XSegment getSegment() {
    return segment;
  }

  public void setSegment(XSegment segment) {
    this.segment = segment;
  }

  public Filter getFilter() {
    return filter;
  }

  public void setFilter(Filter filter) {
    this.filter = filter;
  }

  public Set<Function> getFunctions() {
    return functions;
  }

  public void setFunctions(Set<Function> functions) {
    this.functions = functions;
  }

  public long getDayDistance() {
    return dayDistance;
  }

  public void setDayDistance(long dayDistance) {
    this.dayDistance = dayDistance;
  }

  public DateTruncateType getDateTruncateType() {
    return dateTruncateType;
  }

  public void setDateTruncateType(DateTruncateType dateTruncateType) {
    this.dateTruncateType = dateTruncateType;
  }

  public String getInputBeginDate() {
    return inputBeginDate;
  }

  public void setInputBeginDate(String inputBeginDate) {
    this.inputBeginDate = inputBeginDate;
  }

  public String getInputEndDate() {
    return inputEndDate;
  }

  public void setInputEndDate(String inputEndDate) {
    this.inputEndDate = inputEndDate;
  }

  public String getKey() {
    if (this.key == null) {
      setKey(toKey());
    }
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public List<UserProp> getUserProperties() {
    return userProperties;
  }

  public void setUserProperties(List<UserProp> userProperties) {
    this.userProperties = userProperties;
  }

  public String getSqlSegments() {
    return sqlSegments;
  }

  public void setSqlSegments(String sqlSegments) {
    this.sqlSegments = sqlSegments;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((event == null) ? 0 : event.hashCode());
    result = prime * result + ((filter == null) ? 0 : filter.hashCode());
    result = prime * result + ((projectId == null) ? 0 : projectId.hashCode());
    result = prime * result + ((realBeginDate == null) ? 0 : realBeginDate.hashCode()
    );
    result = prime * result + ((realEndDate == null) ? 0 : realEndDate.hashCode()
    );
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
    FormulaQueryDescriptor other = (FormulaQueryDescriptor) obj;
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
    if (projectId == null) {
      if (other.projectId != null)
        return false;
    } else if (!projectId.equals(other.projectId))
      return false;
    if (realBeginDate == null) {
      if (other.realBeginDate != null)
        return false;
    } else if (!realBeginDate.equals(other.realBeginDate))
      return false;
    if (realEndDate == null) {
      if (other.realEndDate != null)
        return false;
    } else if (!realEndDate.equals(other.realEndDate))
      return false;
    if (segment == null) {
      if (other.segment != null)
        return false;
    } else if (!segment.equals(other.segment))
      return false;
    return true;
  }
}
