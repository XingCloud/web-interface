package com.xingcloud.webinterface.utils;

import static com.xingcloud.webinterface.enums.Interval.HOUR;
import static com.xingcloud.webinterface.enums.Interval.MIN5;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.TIME_UNIT_HOUR;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.TIME_UNIT_MIN5;

import com.xingcloud.adhocprocessorV2.hbase.model.CopResultV2;
import com.xingcloud.adhocprocessorV2.query.model.FormulaQueryDescriptor.GroupByQueryType;
import com.xingcloud.webinterface.enums.CommonQueryType;
import com.xingcloud.webinterface.enums.GroupByType;
import com.xingcloud.webinterface.enums.Interval;
import com.xingcloud.webinterface.enums.Operator;
import com.xingcloud.webinterface.model.Filter;
import com.xingcloud.webinterface.model.ResultTuple;
import com.xingcloud.webinterface.model.formula.CommonFormulaQueryDescriptor;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import com.xingcloud.webinterface.model.formula.GroupByFormulaQueryDescriptor;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * 内部类型转换工具
 *
 * @author Z J Wu @ 2012-10-23 [All ur base r belong 2 us]
 */
public class ConvertUtils {

  /**
   * 王宇飞的CopResultV2转换成Number数组, 消除调用Cache包时对王宇飞的类的依赖
   *
   * @param data
   * @return
   */
  // FIXME CopResultV2中, 事件次数, 和返回的是大Long, 用户数是小long, 导致无法区分空, 应该都是大Long
  public static Map<String, Number[]> copResult2NumberArrays(Map<Object, CopResultV2> data) {
    if (data == null) {
      return null;
    }

    Map<String, Number[]> result = new HashMap<String, Number[]>(data.size());
    String k = null;
    Number[] v = null;
    CopResultV2 copResult = null;
    for (Entry<Object, CopResultV2> entry : data.entrySet()) {
      k = entry.getKey().toString();
      copResult = entry.getValue();

      v = new Number[4];
      v[0] = copResult.getEventNum();
      v[1] = copResult.getEventAmount();
      v[2] = copResult.getUserNum();
      v[3] = copResult.getPercent();
      result.put(k, v);
    }
    return result;
  }

  public static ResultTuple copResult2ResultTuple(CopResultV2 copr) {
    if (copr == null) {
      return null;
    }
    ResultTuple rt = new ResultTuple(copr.getEventNum(), copr.getEventAmount(), copr.getUserNum(), copr.getPercent());
    return rt;
  }

  public static Map<Object, ResultTuple> copResultMap2ResultTupleMap(Map<Object, CopResultV2> copMap) {
    if (copMap == null) {
      return null;
    }
    Map<Object, ResultTuple> m = new HashMap<Object, ResultTuple>(copMap.size());
    for (Entry<Object, CopResultV2> entry : copMap.entrySet()) {
      m.put(entry.getKey(), copResult2ResultTuple(entry.getValue()));
    }
    return m;
  }

  public static com.xingcloud.adhocprocessorV2.util.Constants.Operator convertOperatorOfFilter(Operator o) {
    switch (o) {
      case ALL:
        return com.xingcloud.adhocprocessorV2.util.Constants.Operator.ALL;
      case BETWEEN:
        return com.xingcloud.adhocprocessorV2.util.Constants.Operator.BETWEEN;
      case EQ:
        return com.xingcloud.adhocprocessorV2.util.Constants.Operator.EQ;
      case GTE:
        return com.xingcloud.adhocprocessorV2.util.Constants.Operator.GE;
      case GT:
        return com.xingcloud.adhocprocessorV2.util.Constants.Operator.GT;
      case LTE:
        return com.xingcloud.adhocprocessorV2.util.Constants.Operator.LE;
      case LT:
        return com.xingcloud.adhocprocessorV2.util.Constants.Operator.LT;
      case NE:
        return com.xingcloud.adhocprocessorV2.util.Constants.Operator.NE;
      default:
        return null;
    }
  }

  public static com.xingcloud.adhocprocessorV2.query.model.Filter convertFilter(Filter filter) {
    com.xingcloud.adhocprocessorV2.query.model.Filter f = new com.xingcloud.adhocprocessorV2.query.model.Filter();
    f.setOperator(convertOperatorOfFilter(filter.getOperator()));
    f.setValue(filter.getValue());
    f.setExtraValue(filter.getExtraValue());
    return f;
  }

  public static com.xingcloud.adhocprocessorV2.query.model.FormulaQueryDescriptor.Interval convertInterval(
    Interval interval) {
    switch (interval) {
      case MIN5:
        return com.xingcloud.adhocprocessorV2.query.model.FormulaQueryDescriptor.Interval.MIN5;
      case HOUR:
        return com.xingcloud.adhocprocessorV2.query.model.FormulaQueryDescriptor.Interval.HOUR;
      case DAY:
        return com.xingcloud.adhocprocessorV2.query.model.FormulaQueryDescriptor.Interval.DAY;
      case WEEK:
        return com.xingcloud.adhocprocessorV2.query.model.FormulaQueryDescriptor.Interval.WEEK;
      case MONTH:
        return com.xingcloud.adhocprocessorV2.query.model.FormulaQueryDescriptor.Interval.MONTH;
      case PERIOD:
        return com.xingcloud.adhocprocessorV2.query.model.FormulaQueryDescriptor.Interval.PERIOD;
      default:
        return null;
    }
  }

  public static com.xingcloud.adhocprocessorV2.query.model.FormulaQueryDescriptor convertDescriptorInWebInterface2ADH(
    FormulaQueryDescriptor fqd) {
    com.xingcloud.adhocprocessorV2.query.model.FormulaQueryDescriptor adhFqd = null;
    String projectId = fqd.getProjectId();
    String beginDate = fqd.getRealBeginDate();
    String endDate = fqd.getRealEndDate();
    String event = fqd.getEvent();
    String segment = fqd.getSegment();
    Filter filter = fqd.getFilter();
    com.xingcloud.adhocprocessorV2.query.model.Filter adhFilter = convertFilter(filter);

    boolean needCount = fqd.isNeedCountFunction();
    boolean needSum = fqd.isNeedSumFunction();
    boolean needUserNum = fqd.isNeedUserNumFunction();

    if (fqd instanceof CommonFormulaQueryDescriptor) {
      CommonFormulaQueryDescriptor cfqd = (CommonFormulaQueryDescriptor) fqd;

      CommonQueryType cqt = cfqd.getCommonQueryType();
      com.xingcloud.adhocprocessorV2.query.model.FormulaQueryDescriptor.CommonQueryType adhCqt = null;
      switch (cqt) {
        case NORMAL:
          adhCqt = com.xingcloud.adhocprocessorV2.query.model.FormulaQueryDescriptor.CommonQueryType.NORMAL;
          break;
        case TOTAL:
          adhCqt = com.xingcloud.adhocprocessorV2.query.model.FormulaQueryDescriptor.CommonQueryType.TOTAL;
          break;
        case NATURAL:
          adhCqt = com.xingcloud.adhocprocessorV2.query.model.FormulaQueryDescriptor.CommonQueryType.NATURAL;
          break;
        default:
          break;
      }

      Interval interval = cfqd.getInterval();
      com.xingcloud.adhocprocessorV2.query.model.FormulaQueryDescriptor.Interval adhInterval = convertInterval(
        interval);
      if (HOUR.equals(interval)) {
        adhFqd = new com.xingcloud.adhocprocessorV2.query.model.FormulaQueryDescriptor(projectId, beginDate, endDate,
                                                                                       event, segment, adhFilter, null,
                                                                                       null, needCount, needSum,
                                                                                       needUserNum,
                                                                                       GroupByQueryType.PERIOD,
                                                                                       TIME_UNIT_HOUR, adhInterval);
      } else if (MIN5.equals(interval)) {
        adhFqd = new com.xingcloud.adhocprocessorV2.query.model.FormulaQueryDescriptor(projectId, beginDate, endDate,
                                                                                       event, segment, adhFilter, null,
                                                                                       null, needCount, needSum,
                                                                                       needUserNum,
                                                                                       GroupByQueryType.PERIOD,
                                                                                       TIME_UNIT_MIN5, adhInterval);
      } else {
        adhFqd = new com.xingcloud.adhocprocessorV2.query.model.FormulaQueryDescriptor(projectId, beginDate, endDate,
                                                                                       event, segment, adhFilter, null,
                                                                                       null, needCount, needSum,
                                                                                       needUserNum, adhCqt,
                                                                                       adhInterval);
      }
    } else {
      GroupByFormulaQueryDescriptor gbfqd = (GroupByFormulaQueryDescriptor) fqd;
      String groupBy = gbfqd.getGroupBy();
      GroupByType groupByType = gbfqd.getGroupByType();
      switch (groupByType) {
        case EVENT:
          adhFqd = new com.xingcloud.adhocprocessorV2.query.model.FormulaQueryDescriptor(projectId, beginDate, endDate,
                                                                                         event, segment, adhFilter,
                                                                                         null, null, needCount, needSum,
                                                                                         needUserNum,
                                                                                         GroupByQueryType.EVENT,
                                                                                         Integer.valueOf(groupBy),
                                                                                         com.xingcloud.adhocprocessorV2.query.model.FormulaQueryDescriptor.Interval.PERIOD);
          break;
        case USER_PROPERTIES:
          adhFqd = new com.xingcloud.adhocprocessorV2.query.model.FormulaQueryDescriptor(projectId, beginDate, endDate,
                                                                                         event, segment, adhFilter,
                                                                                         null, null, needCount, needSum,
                                                                                         needUserNum, groupBy,
                                                                                         com.xingcloud.adhocprocessorV2.query.model.FormulaQueryDescriptor.Interval.PERIOD);
          break;
        default:
          break;
      }
    }

    return adhFqd;
  }

}
