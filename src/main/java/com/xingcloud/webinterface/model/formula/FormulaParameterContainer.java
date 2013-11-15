package com.xingcloud.webinterface.model.formula;

import static com.xingcloud.basic.utils.DateUtils.short2Date;

import com.google.common.base.Strings;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import com.xingcloud.webinterface.annotation.Ignore;
import com.xingcloud.webinterface.annotation.JsonName;
import com.xingcloud.webinterface.enums.Interval;
import com.xingcloud.webinterface.enums.QueryType;
import com.xingcloud.webinterface.enums.SliceType;
import com.xingcloud.webinterface.exception.NumberOfDayException;
import com.xingcloud.webinterface.exception.ParseJsonException;
import com.xingcloud.webinterface.exception.UnsupportedFormulaQueryItemException;
import com.xingcloud.webinterface.exception.XParameterException;
import com.xingcloud.webinterface.model.Filter;
import com.xingcloud.webinterface.utils.DateSplitter;
import com.xingcloud.webinterface.utils.WebInterfaceConstants;
import net.sf.json.JSONArray;
import net.sf.json.JSONNull;
import net.sf.json.JSONObject;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class FormulaParameterContainer implements Serializable {
  @Ignore(true)
  private static final long serialVersionUID = 5702785052779055259L;

  @SerializedName("id")
  @JsonName("id")
  private String id;

  @Expose
  @SerializedName("project_id")
  @JsonName("project_id")
  private String projectId;

  @Expose
  @SerializedName("start_time")
  @JsonName("start_time")
  private String beginDate;

  @Expose
  @SerializedName("end_time")
  @JsonName("end_time")
  private String endDate;

  @Expose
  @SerializedName("interval")
  @JsonName("interval")
  private Interval interval;

  @Expose
  @SerializedName("formula")
  @JsonName("formula")
  private String formula;

  @Expose
  @SerializedName("items")
  @JsonName("items")
  private List<FormulaParameterItem> items;

  @Expose
  @SerializedName("slice_pattern")
  @JsonName("slice_pattern")
  private String slicePattern;

  @Expose
  @SerializedName("slice_type")
  @JsonName("slice_type")
  private SliceType sliceType;

  @JsonName("type")
  public QueryType queryType;

  public FormulaParameterContainer() {
  }

  public FormulaParameterContainer(String id, String projectId, String beginDate, String endDate, Interval interval,
                                   String formula, List<FormulaParameterItem> items, String slicePattern,
                                   QueryType queryType) {
    super();
    this.id = id;
    this.projectId = projectId;
    this.beginDate = beginDate;
    this.endDate = endDate;
    this.interval = interval;
    this.formula = formula;
    this.items = items;
    this.slicePattern = slicePattern;
    this.queryType = queryType;
  }

  public boolean validate() throws XParameterException, NumberOfDayException {
    return validate(false);
  }

  public boolean validate(boolean ignore) throws XParameterException, NumberOfDayException {
    if (!ignore) {
      if (Strings.isNullOrEmpty(getId())) {
        throw new XParameterException("Container has no id.");
      }
      if (Strings.isNullOrEmpty(getProjectId())) {
        throw new XParameterException("Container has no project id.");
      }
      String bDate = getBeginDate();
      String eDate = getEndDate();
      if (Strings.isNullOrEmpty(bDate) || Strings.isNullOrEmpty(eDate)) {
        throw new XParameterException("Begin date or end date is null");
      }
      try {
        Date d1 = short2Date(bDate);
        Date d2 = short2Date(eDate);
        if (d1.after(d2)) {
          throw new XParameterException("Begin date cannot be after end date(Begin=" + bDate + ",End=" + eDate + ").");
        }
      } catch (ParseException e) {
        throw new XParameterException("Cannot parse begin & end date.", e);
      }

      if (Strings.isNullOrEmpty(formula)) {
        throw new XParameterException("Formula cannot be null.");
      }
    }

    if (CollectionUtils.isEmpty(items)) {
      throw new XParameterException("Query item cannot be null.");
    }
    if (this.queryType == null) {
      throw new XParameterException("Query type cannot be null.");
    }

    for (FormulaParameterItem item : this.items) {
      item.validate(this);
    }

    Interval interval = getInterval();
    switch (interval) {
      case MIN5:
        break;
      case HOUR:
        break;
      case DAY:
        break;
      case WEEK:
        break;
      case MONTH:
        break;
      default:
        break;
    }

    return true;
  }

  public boolean needCalculate() {
    return !CollectionUtils.isEmpty(items) && items.size() > 1;
  }

  public List<String> getQueryDateSlice() throws ParseException {
    return DateSplitter.split(getBeginDate(), getEndDate(), getInterval());
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((beginDate == null) ? 0 : beginDate.hashCode());
    result = prime * result + ((endDate == null) ? 0 : endDate.hashCode());
    result = prime * result + ((formula == null) ? 0 : formula.hashCode());
    result = prime * result + ((id == null) ? 0 : id.hashCode());
    result = prime * result + ((interval == null) ? 0 : interval.hashCode());
    result = prime * result + ((items == null) ? 0 : items.hashCode());
    result = prime * result + ((projectId == null) ? 0 : projectId.hashCode());
    result = prime * result + ((queryType == null) ? 0 : queryType.hashCode());
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
    FormulaParameterContainer other = (FormulaParameterContainer) obj;
    if (beginDate == null) {
      if (other.beginDate != null)
        return false;
    } else if (!beginDate.equals(other.beginDate))
      return false;
    if (endDate == null) {
      if (other.endDate != null)
        return false;
    } else if (!endDate.equals(other.endDate))
      return false;
    if (formula == null) {
      if (other.formula != null)
        return false;
    } else if (!formula.equals(other.formula))
      return false;
    if (id == null) {
      if (other.id != null)
        return false;
    } else if (!id.equals(other.id))
      return false;
    if (interval != other.interval)
      return false;
    if (items == null) {
      if (other.items != null)
        return false;
    } else if (!items.equals(other.items))
      return false;
    if (projectId == null) {
      if (other.projectId != null)
        return false;
    } else if (!projectId.equals(other.projectId))
      return false;
    if (queryType != other.queryType)
      return false;
    return true;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getProjectId() {
    return projectId;
  }

  public void setProjectId(String projectId) {
    this.projectId = projectId;
  }

  public String getBeginDate() {
    return beginDate;
  }

  public void setBeginDate(String beginDate) {
    this.beginDate = beginDate;
  }

  public String getEndDate() {
    return endDate;
  }

  public void setEndDate(String endDate) {
    this.endDate = endDate;
  }

  public Interval getInterval() {
    return interval;
  }

  public void setInterval(Interval interval) {
    this.interval = interval;
  }

  public String getFormula() {
    return formula;
  }

  public void setFormula(String formula) {
    this.formula = formula;
  }

  public List<FormulaParameterItem> getItems() {
    return items;
  }

  public void setItems(List<FormulaParameterItem> items) {
    this.items = items;
  }

  public QueryType getQueryType() {
    return queryType;
  }

  public void setQueryType(QueryType queryType) {
    this.queryType = queryType;
  }

  public String getSlicePattern() {
    return slicePattern;
  }

  public void setSlicePattern(String slicePattern) {
    this.slicePattern = slicePattern;
  }

  public SliceType getSliceType() {
    return sliceType;
  }

  public void setSliceType(SliceType sliceType) {
    this.sliceType = sliceType;
  }

  @Override
  public String toString() {
    return "FPC(" + queryType + ")." + id + "." + projectId + "." + beginDate + "." + endDate + "." + interval + "." + formula + ".(" + slicePattern + ")." + items;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public static Filter json2Filter(JSONObject jo) throws IllegalArgumentException, IllegalAccessException,
    InstantiationException {
    if (jo == null) {
      return null;
    }
    Class<Filter> clz = Filter.class;
    Field[] fields = clz.getDeclaredFields();
    Class typeClz;
    JsonName jn;
    String fieldName, jsonName;
    Object value;
    Filter f = clz.newInstance();
    for (Field field : fields) {
      jn = field.getAnnotation(JsonName.class);
      fieldName = field.getName();
      jsonName = ((jn == null) ? fieldName : jn.value());
      value = jo.get(jsonName);
      if (value == null || (value instanceof JSONNull)) {
        continue;
      }
      field.setAccessible(true);
      typeClz = field.getType();
      if (typeClz.isEnum()) {
        field.set(f, Enum.valueOf(typeClz, value.toString()));
      } else {
        field.set(f, value);
      }
    }
    return f;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public static List<FormulaParameterItem> json2Items(QueryType qt, JSONArray ja) throws IllegalArgumentException,
    IllegalAccessException, InstantiationException, UnsupportedFormulaQueryItemException {
    if (ja == null || ja.size() == 0) {
      return null;
    }
    List<FormulaParameterItem> list = new ArrayList<FormulaParameterItem>(ja.size());
    Class clz, superClz;
    if (qt == null) {
      throw new UnsupportedFormulaQueryItemException("Null query type");
    }
    switch (qt) {
      case COMMON:
        clz = CommonFormulaParameterItem.class;
        break;
      case GROUP:
        clz = GroupByFormulaParameterItem.class;
        break;
      default:
        throw new UnsupportedFormulaQueryItemException("what is " + qt + "?");
    }
    superClz = clz.getSuperclass();

    Field[] superFields = superClz.getDeclaredFields();
    Field[] fields = clz.getDeclaredFields();
    JsonName jn;
    String fieldName, jsonName;
    Object value;
    Class typeClz;
    JSONObject jo;
    FormulaParameterItem instance;
    Filter f;
    for (int i = 0; i < ja.size(); i++) {
      jo = ja.getJSONObject(i);
      try {
        instance = (FormulaParameterItem) clz.newInstance();
      } catch (Exception e1) {
        e1.printStackTrace();
        continue;
      }
      for (Field field : superFields) {
        jn = field.getAnnotation(JsonName.class);
        fieldName = field.getName();
        jsonName = ((jn == null) ? fieldName : jn.value());
        value = jo.get(jsonName);
        if (value == null || (value instanceof JSONNull)) {
          continue;
        }
        field.setAccessible(true);
        if ("filter".equals(jsonName) && (value instanceof JSONObject)) {
          f = json2Filter((JSONObject) value);
          field.set(instance, f);
        } else {
          typeClz = field.getType();
          if (typeClz.isEnum()) {
            field.set(instance, Enum.valueOf(typeClz, value.toString()));
          } else {
            if (value instanceof JSONObject) {
              field.set(instance, value.toString());
            } else {
              field.set(instance, value);
            }
          }
        }
      }
      for (Field field : fields) {
        jn = field.getAnnotation(JsonName.class);
        fieldName = field.getName();
        jsonName = ((jn == null) ? fieldName : jn.value());
        value = jo.get(jsonName);
        if (value == null || (value instanceof JSONNull)) {
          continue;
        }
        field.setAccessible(true);
        if (!(value instanceof JSONArray)) {
          typeClz = field.getType();
          if (typeClz.isEnum()) {
            field.set(instance, Enum.valueOf(typeClz, value.toString()));
          } else {
            if (value instanceof JSONObject) {
              field.set(instance, value.toString());
            } else {
              field.set(instance, value);
            }
          }
        }
      }
      list.add(instance);
    }

    return list;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public static List<FormulaParameterContainer> json2Containers(String json) throws ParseJsonException {
    if (Strings.isNullOrEmpty(json)) {
      throw new ParseJsonException("Json string is null");
    }

    JSONArray ja = JSONArray.fromObject(json);

    int size = ja.size();
    if (size == 0) {
      throw new ParseJsonException("Json string is empty");
    }

    Class<FormulaParameterContainer> clz = FormulaParameterContainer.class;
    Field[] fields = clz.getDeclaredFields();

    JsonName jn;
    Ignore ignore;
    String fieldName, jsonName;
    Object value;
    Class typeClz;
    JSONObject jo;
    JSONArray metricArray = null;
    FormulaParameterContainer instance;
    List<FormulaParameterContainer> containers = new ArrayList<FormulaParameterContainer>(size);
    List<FormulaParameterItem> items;
    for (int i = 0; i < size; i++) {
      jo = ja.getJSONObject(i);
      try {
        instance = clz.newInstance();
      } catch (Exception e1) {
        throw new ParseJsonException(e1);
      }
      for (Field field : fields) {
        ignore = field.getAnnotation(Ignore.class);
        if (ignore != null && ignore.value()) {
          continue;
        }
        jn = field.getAnnotation(JsonName.class);
        fieldName = field.getName();
        jsonName = ((jn == null) ? fieldName : jn.value());
        value = jo.get(jsonName);
        if (value == null || (value instanceof JSONNull)) {
          continue;
        }

        field.setAccessible(true);
        try {
          if (value instanceof JSONArray) {
            metricArray = (JSONArray) value;
          } else {
            typeClz = field.getType();
            if (typeClz.isEnum()) {
              field.set(instance, Enum.valueOf(typeClz, value.toString()));
            } else {
              field.set(instance, value);
            }
          }
        } catch (Exception e) {
          throw new ParseJsonException(e);
        }
      }
      try {
        items = json2Items(instance.getQueryType(), metricArray);
      } catch (Exception e) {
        throw new ParseJsonException(e);
      }
      instance.setItems(items);
      containers.add(instance);
    }
    return containers;
  }

  public void init() throws XParameterException {
    String pid;
    if (Strings.isNullOrEmpty(getProjectId())) {
      pid = WebInterfaceConstants.DEFAULT_PROJECT_ID_IDENTIFIER;
    } else {
      pid = getProjectId().toLowerCase();
    }
    setProjectId(pid);

    if (getInterval() == null) {
      setInterval(Interval.DAY);
    }

    String sp = getSlicePattern();
    setSlicePattern(StringUtils.trimToNull(sp));
    List<FormulaParameterItem> items = getItems();
    for (FormulaParameterItem item : items) {
      item.init(this);
    }
  }

}
