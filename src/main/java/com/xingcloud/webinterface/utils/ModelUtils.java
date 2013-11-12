package com.xingcloud.webinterface.utils;

import static com.xingcloud.basic.Constants.DEFAULT_TIME_ZONE;
import static com.xingcloud.basic.utils.DateUtils.before;
import static com.xingcloud.basic.utils.DateUtils.date2Short;
import static com.xingcloud.basic.utils.DateUtils.dateSubtraction;
import static com.xingcloud.basic.utils.DateUtils.previousDay;
import static com.xingcloud.basic.utils.DateUtils.short2Date;
import static com.xingcloud.mysql.PropType.sql_datetime;
import static com.xingcloud.webinterface.conf.WebInterfaceConfig.DEBUG;
import static com.xingcloud.webinterface.enums.DateTruncateLevel.LOOSELY;
import static com.xingcloud.webinterface.enums.Function.SUM;
import static com.xingcloud.webinterface.enums.Function.USER_NUM;
import static com.xingcloud.webinterface.enums.MongoOperator.LT;
import static com.xingcloud.webinterface.enums.MongoOperator.LTE;
import static com.xingcloud.webinterface.sql.SqlUtilsConstants.DATE_FIELD;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.DEFAULT_SAMPLING_RATE;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.TOTAL_USER;

import com.google.common.base.Strings;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import com.xingcloud.mysql.PropType;
import com.xingcloud.mysql.UserProp;
import com.xingcloud.webinterface.enums.DateTruncateLevel;
import com.xingcloud.webinterface.enums.Function;
import com.xingcloud.webinterface.enums.Interval;
import com.xingcloud.webinterface.enums.Operator;
import com.xingcloud.webinterface.exception.ParseIncrementalException;
import com.xingcloud.webinterface.exception.SegmentException;
import com.xingcloud.webinterface.exception.UserPropertyException;
import com.xingcloud.webinterface.exception.XParameterException;
import com.xingcloud.webinterface.model.BeginEndDatePair;
import com.xingcloud.webinterface.model.formula.FormulaParameterItem;
import com.xingcloud.webinterface.segment.XSegment;
import com.xingcloud.webinterface.sql.desc.ConditionUnit;
import com.xingcloud.webinterface.sql.desc.JoinDescriptor;
import com.xingcloud.webinterface.sql.desc.SegmentDescriptor;
import com.xingcloud.webinterface.sql.desc.TableDescriptor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class ModelUtils {

  public static DateTruncateLevel getDateTruncateLeve(boolean isAverage, boolean isAccumulative) {
    // if (isAverage) {
    // return STRICTLY;
    // }
    return LOOSELY;
  }

  public static boolean isAverage(Integer nd, Integer ndo) {
    return hasNDorNDO(nd, ndo);
  }

  public static boolean isAccumulative(String projectId, Interval interval, FormulaParameterItem item) throws
    SegmentException {
    Function function = item.getFunction();
        /*
         * Annotated By Z J Wu@2013-02-27
         * 
         * 现在, 由于针对小时和五分钟, handler不在处理(在后台处理, 生成一堆细化到小时/五分钟的小segment),
         * 因此让小时/5分钟的查询也变得"不变动属性可累加", 即具有和Day相同的判定逻辑
         * 
         */
    // if (item instanceof CommonFormulaParameterItem) {
    // float days = interval.getDays();
    // if (days < 1) {
    // return !USER_NUM.equals(function);
    // }
    // }

    String segment = item.getSegment();
    try {
      if (DEBUG) {
        return (!USER_NUM.equals(function));
      }
      /*
      TODO Annotated by Z J Wu @ 2013-7-3
      因为目前segment变成了复杂的表达式, 表达式的内容如果涉及到日期, 特别是带有date_add这样
      的函数, 会使得segment取值有可能变成相互交叉的范围, 故不再判断segment可加性
      如某查询, 查询1-5号的数据, segment如下
      {
        "register_time": [
          {
            "op": "ge",
            "expr": "$date_add(0)",
            "type": "VAR"
          },
          {
            "op": "le",
            "expr": "$date_add(3)",
            "type": "VAR"
          }
        ]
      }
      每个数据点的segment是1-4, 2-5, 3-6, 4-7, 5-8
      这些数据点在日期上相互交叉, 显然不可累加
       */
//      return (!USER_NUM.equals(function)) || immobile(projectId, segment);
      return (!USER_NUM.equals(function));
    } catch (Exception e) {
      throw new SegmentException(e);
    }
  }

  public static BeginEndDatePair getRealBeginEndDatePair(String beginDate, String endDate, Integer nd,
                                                         Integer ndo) throws XParameterException {
    if (Strings.isNullOrEmpty(beginDate)) {
      throw new XParameterException("Cannot parse real begin and end date pair because begin date is empty.");
    }

    Date d1, d2;
    BeginEndDatePair pair;
    if (hasUsefulNDorNDO(ndo) || hasUsefulNDorNDO(nd)) {
      if (Strings.isNullOrEmpty(endDate)) {
        throw new RuntimeException(
          "Cannot parse real begin and end date pair with number of day because end date is empty.");
      }
      if (nd == null) {
        throw new RuntimeException("Number of day is null while useful nd or ndo exsits - ND=" + nd + ", NDO=" + ndo);
      }
      if (ndo == null) {
        throw new RuntimeException(
          "Number of day origin is null while useful nd or ndo exsits - ND=" + nd + ", NDO=" + ndo);
      }
      try {
        d1 = short2Date(dateSubtraction(beginDate, ndo));
        d2 = short2Date(dateSubtraction(endDate, nd));
      } catch (ParseException e) {
        throw new XParameterException(e);
      }
      if (d1.after(d2)) {
        Date tmp = d1;
        d1 = d2;
        d2 = tmp;
      }
      pair = new BeginEndDatePair(date2Short(d1), date2Short(d2));
    } else {
      pair = new BeginEndDatePair(beginDate, beginDate);
    }
    return pair;
  }

  public static boolean hasVolatileEventProperty(TableDescriptor td, Date targetDate) throws SegmentException,
    ParseException {
    Map<String, List<ConditionUnit>> tdMap = td.getConditionUnits();
    String compareDate, valueDateString, tDate = date2Short(targetDate);
    List<ConditionUnit> cus = tdMap.get(DATE_FIELD);
    if (CollectionUtils.isEmpty(cus)) {
      throw new SegmentException("Cannot parse event table is volatile or not because condition map is empty - " + td);
    }
    Operator operator;
    for (ConditionUnit cu : cus) {
      operator = cu.getOperator();
      valueDateString = cu.getValueObject().toString();
      if (Operator.LT.equals(operator)) {
        compareDate = previousDay(valueDateString);
      } else {
        compareDate = valueDateString;
      }
      if (!before(compareDate, tDate)) {
        return true;
      }
    }
    return false;
  }

  public static boolean hasVolatileUserProperty(String projectId, TableDescriptor td, Date targetDate) throws
    UserPropertyException, ParseException {
    Map<String, List<ConditionUnit>> tdMap = td.getConditionUnits();
    UserProp up;
    PropType pt;
    String propertyName, compareDate, valueDateString, tDate = date2Short(targetDate);
    List<ConditionUnit> cus;
    Operator operator;
    for (Entry<String, List<ConditionUnit>> entry : tdMap.entrySet()) {
      propertyName = entry.getKey();
      up = UserPropertiesInfoManager.getInstance().getUserProp(projectId, propertyName);
      pt = up.getPropType();

      if (sql_datetime.equals(pt)) {
        cus = entry.getValue();
        for (ConditionUnit cu : cus) {
          operator = cu.getOperator();
          valueDateString = cu.getValueObject().toString();
          if (Operator.LT.equals(operator)) {
            compareDate = previousDay(valueDateString);
          } else {
            compareDate = valueDateString;
          }
          if (!before(compareDate, tDate)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  public static boolean hasVolatileProperty(String projectId, XSegment segment, Date targetDate) throws
    SegmentException {
    try {
      return _hasVolatileProperty(projectId, segment, targetDate);
    } catch (ParseException e) {
      throw new SegmentException(e);
    } catch (UserPropertyException e) {
      throw new SegmentException(e);
    }
  }

  private static boolean _hasVolatileProperty(String projectId, XSegment segment, Date targetDate) throws
    SegmentException, ParseException, UserPropertyException {
    // 无用户群
    if (segment == null) {
      return false;
    }
    SegmentDescriptor sd = segment.getSegmentDescriptor();
    Set<JoinDescriptor> jds = sd.getJoins();
    TableDescriptor td1, td2;
    if (CollectionUtils.isNotEmpty(jds)) {
      for (JoinDescriptor jd : jds) {
        td1 = jd.getLeft();
        if (td1.isEventTable()) {
          if (hasVolatileEventProperty(td1, targetDate)) {
            return true;
          }
        } else {
          if (hasVolatileUserProperty(projectId, td1, targetDate)) {
            return true;
          }
        }
        td2 = jd.getRight();
        if (td2.isEventTable()) {
          if (hasVolatileEventProperty(td2, targetDate)) {
            return true;
          }
        } else {
          if (hasVolatileUserProperty(projectId, td2, targetDate)) {
            return true;
          }
        }
      }
    }

    List<TableDescriptor> events = sd.getEvent();
    if (CollectionUtils.isNotEmpty(events)) {
      for (TableDescriptor td : events) {
        if (hasVolatileEventProperty(td, targetDate)) {
          return true;
        }
      }
    }

    TableDescriptor userTD = sd.getUser();
    if (userTD != null && hasVolatileUserProperty(projectId, userTD, targetDate)) {
      return true;
    }

    return false;
  }

  public static boolean hasVolatileUserProperty(String projectId, String segment, Date targetDate) throws
    SegmentException, UserPropertyException {
    if (Strings.isNullOrEmpty(segment) || TOTAL_USER.equals(segment)) {
      return false;
    }

    DBObject dbo;
    Map<Object, Object> m;
    try {
      dbo = (DBObject) JSON.parse(segment);
      m = dbo.toMap();
    } catch (Exception e) {
      throw new SegmentException(e);
    }
    if (MapUtils.isEmpty(m)) {
      throw new SegmentException("SegmentPart is not valid.");
    }
    Object propName, propValue;
    UserProp up;
    PropType pt;
    String compareDate;
    for (Entry<Object, Object> entry : m.entrySet()) {
      propName = entry.getKey();
      up = UserPropertiesInfoManager.getInstance().getUserProp(projectId, propName.toString());
      pt = up.getPropType();
      if (sql_datetime.equals(pt)) {
        propValue = entry.getValue();

        if (propValue == null) {
          continue;
        }

        // 复杂型 如"property":{"$gte":"2013-01-10","$lte","2013-01-20"}
        if (propValue instanceof DBObject) {
          DBObject propDBO = (DBObject) propValue;
          if (propDBO.containsField(LT.getOperator())) {
            compareDate = (String) propDBO.get(LT.getOperator());
            try {
              compareDate = previousDay(compareDate);
            } catch (ParseException e) {
              throw new UserPropertyException(e);
            }
          } else if (propDBO.containsField(LTE.getOperator())) {
            compareDate = (String) propDBO.get(LTE.getOperator());
          } else {
            continue;
          }
        }
        // 其余的都当做简单型: "property":"2013-01-10"
        else {
          compareDate = propValue.toString();
        }
        try {
          if (!before(compareDate, date2Short(targetDate))) {
            return true;
          }
        } catch (ParseException e) {
          throw new SegmentException(e);
        }
      }
    }

    return false;
  }

  public static boolean isIncremental(long targetTimeStamp, String realBeginDate, String realEndDate, String projectId,
                                      XSegment segment) throws ParseIncrementalException {
    if (DEBUG) {
      return true;
    }

    if (Strings.isNullOrEmpty(realBeginDate) || Strings.isNullOrEmpty(realEndDate)) {
      return false;
    }
    Calendar c = Calendar.getInstance();
    c.setTimeInMillis(targetTimeStamp);
    c.setTimeZone(DEFAULT_TIME_ZONE);
    c.set(Calendar.HOUR_OF_DAY, 0);
    c.set(Calendar.MINUTE, 0);
    c.set(Calendar.SECOND, 0);
    c.set(Calendar.MILLISECOND, 0);

    Date targetDate = c.getTime();
    Date d1, d2;
    try {
      d1 = short2Date(realBeginDate);
      d2 = short2Date(realEndDate);
    } catch (Exception e) {
      throw new ParseIncrementalException(e);
    }

    if (!(d1.before(targetDate) && d2.before(targetDate))) {
      return false;
    }

    try {
//      if (hasVolatileUserProperty(projectId, segment, targetDate)) {
//        return false;
//      }
      if (hasVolatileProperty(projectId, segment, targetDate)) {
        return false;
      }
    } catch (Exception e) {
      throw new ParseIncrementalException(e);
    }
    return true;
  }

  public static boolean hasNDorNDO(Integer ndOrNdo) {
    return ndOrNdo != null;
  }

  public static boolean hasNDorNDO(Integer... ndOrNdo) {
    for (Integer i : ndOrNdo) {
      if (hasNDorNDO(i)) {
        return true;
      }
    }
    return false;
  }

  public static boolean hasUsefulNDorNDO(Integer ndOrNdo) {
    return ndOrNdo != null && ndOrNdo != 0;
  }

  public static boolean hasUsefulNDorNDO(Integer... ndOrNdo) {
    for (Integer i : ndOrNdo) {
      if (hasUsefulNDorNDO(i)) {
        return true;
      }
    }
    return false;
  }

  public static double resolveSamplingRateByEvent(String event, Function function) {
    if (Strings.isNullOrEmpty(event)) {
      return DEFAULT_SAMPLING_RATE;
    }
    if (!SUM.equals(function)) {
      return DEFAULT_SAMPLING_RATE;
    }
    if (event.contains("pay") || event.contains("adcalc")) {
      return 1d;
    }
    return DEFAULT_SAMPLING_RATE;
  }

}
