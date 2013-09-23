package com.xingcloud.webinterface.utils;

import static com.xingcloud.basic.Constants.INTERNAL_NA;
import static com.xingcloud.basic.utils.DateUtils.before;
import static com.xingcloud.basic.utils.DateUtils.today;
import static com.xingcloud.basic.utils.DateUtils.yesterday;
import static com.xingcloud.webinterface.conf.WebInterfaceConfig.BATCH_GROUPBY;
import static com.xingcloud.webinterface.conf.WebInterfaceConfig.NEW_SEGMENT;
import static com.xingcloud.webinterface.enums.AggregationPolicy.ACCUMULATION;
import static com.xingcloud.webinterface.enums.AggregationPolicy.ACCUMULATION_EXTEND;
import static com.xingcloud.webinterface.enums.AggregationPolicy.AVERAGE;
import static com.xingcloud.webinterface.enums.AggregationPolicy.AVERAGE_EXTEND;
import static com.xingcloud.webinterface.enums.AggregationPolicy.QUERY;
import static com.xingcloud.webinterface.enums.AggregationPolicy.SAME_AS_QUERY_EXTEND;
import static com.xingcloud.webinterface.enums.CommonQueryType.NATURAL;
import static com.xingcloud.webinterface.enums.CommonQueryType.NORMAL;
import static com.xingcloud.webinterface.enums.CommonQueryType.TOTAL;
import static com.xingcloud.webinterface.enums.DateTruncateLevel.STRICTLY;
import static com.xingcloud.webinterface.enums.DateTruncateType.KILL;
import static com.xingcloud.webinterface.enums.DateTruncateType.PASS;
import static com.xingcloud.webinterface.enums.Function.USER_NUM;
import static com.xingcloud.webinterface.enums.GroupByType.USER_PROPERTIES;
import static com.xingcloud.webinterface.enums.Interval.DAY;
import static com.xingcloud.webinterface.enums.Interval.PERIOD;
import static com.xingcloud.webinterface.enums.QueryType.COMMON;
import static com.xingcloud.webinterface.enums.QueryType.GROUP;
import static com.xingcloud.webinterface.exec.QueryDescriptorTruncater.truncateDate;
import static com.xingcloud.webinterface.monitor.MonitorInfo.MI_DESCRIPTOR_BUILD;
import static com.xingcloud.webinterface.monitor.MonitorInfo.MI_STR_TIME_USE_BUILD_DESCRIPTOR;
import static com.xingcloud.webinterface.monitor.SystemMonitor.putMonitorInfo;
import static com.xingcloud.webinterface.segment.SegmentHandlerChain.createHandlers;
import static com.xingcloud.webinterface.segment.SegmentHandlerChain.handleSegment;
import static com.xingcloud.webinterface.segment2.SegmentEvaluator.evaluate;
import static com.xingcloud.webinterface.utils.DateSplitter.split2Pairs;
import static com.xingcloud.webinterface.utils.ModelUtils.getDateTruncateLeve;
import static com.xingcloud.webinterface.utils.ModelUtils.getRealBeginEndDatePair;
import static com.xingcloud.webinterface.utils.ModelUtils.hasUsefulNDorNDO;
import static com.xingcloud.webinterface.utils.ModelUtils.isAccumulative;
import static com.xingcloud.webinterface.utils.ModelUtils.isAverage;
import static com.xingcloud.webinterface.utils.ModelUtils.resolveSamplingRateByEvent;
import static com.xingcloud.webinterface.utils.WebInterfaceCommonUtils.booleans2Int;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.MIN_HOUR_SUMMARY_POLICY_ARR_NATURAL;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.MIN_HOUR_SUMMARY_POLICY_ARR_TOTAL;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.PERIOD_SUMMARY_POLICY_ARR_NATURAL;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.PERIOD_SUMMARY_POLICY_ARR_TOTAL;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.TOTAL_USER;

import com.google.common.base.Strings;
import com.xingcloud.webinterface.enums.AggregationPolicy;
import com.xingcloud.webinterface.enums.DateTruncateLevel;
import com.xingcloud.webinterface.enums.DateTruncateType;
import com.xingcloud.webinterface.enums.Function;
import com.xingcloud.webinterface.enums.GroupByType;
import com.xingcloud.webinterface.enums.Interval;
import com.xingcloud.webinterface.enums.SliceType;
import com.xingcloud.webinterface.exception.SegmentException;
import com.xingcloud.webinterface.exception.UserPropertyException;
import com.xingcloud.webinterface.exception.XParameterException;
import com.xingcloud.webinterface.model.BeginEndDatePair;
import com.xingcloud.webinterface.model.Filter;
import com.xingcloud.webinterface.model.formula.CommonFormulaParameterItem;
import com.xingcloud.webinterface.model.formula.CommonFormulaQueryDescriptor;
import com.xingcloud.webinterface.model.formula.FormulaParameterContainer;
import com.xingcloud.webinterface.model.formula.FormulaParameterItem;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import com.xingcloud.webinterface.model.formula.GroupByFormulaParameterItem;
import com.xingcloud.webinterface.model.formula.GroupByFormulaQueryDescriptor;
import com.xingcloud.webinterface.model.intermediate.CommonIdResult;
import com.xingcloud.webinterface.model.intermediate.CommonItemResult;
import com.xingcloud.webinterface.model.intermediate.CommonItemResultGroup;
import com.xingcloud.webinterface.model.intermediate.GroupByIdResult;
import com.xingcloud.webinterface.model.intermediate.GroupByItemResult;
import com.xingcloud.webinterface.model.intermediate.GroupByItemResultGroup;
import com.xingcloud.webinterface.monitor.MonitorInfo;
import com.xingcloud.webinterface.segment.SegmentHandler;
import com.xingcloud.webinterface.segment.SegmentSplitter;
import com.xingcloud.webinterface.segment2.SegmentSeparator;
import org.apache.commons.collections.CollectionUtils;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IdResultBuilder {

  public static AggregationPolicy parseTotalSummaryPolicy(int booleanInt, Interval interval) {
    float intervalFloat = interval.getDays();

    // 查询小时/5分钟的情况
    if (intervalFloat < 1) {
      return MIN_HOUR_SUMMARY_POLICY_ARR_TOTAL[booleanInt];
    }
    // 查询天/周/月的情况
    else {
      return PERIOD_SUMMARY_POLICY_ARR_TOTAL[booleanInt];
    }
  }

  public static AggregationPolicy parseNaturalSummaryPolicy(int booleanInt, Interval interval) {
    float intervalFloat = interval.getDays();

    // 查询小时/5分钟的情况
    if (intervalFloat < 1) {
      return MIN_HOUR_SUMMARY_POLICY_ARR_NATURAL[booleanInt];
    }
    // 查询天/周/月的情况
    else {
      return PERIOD_SUMMARY_POLICY_ARR_NATURAL[booleanInt];
    }
  }

  private static Map<String, Function> extractFunctionMap(FormulaParameterContainer container) {
    if (container == null) {
      return null;
    }
    List<FormulaParameterItem> items = container.getItems();
    if (CollectionUtils.isEmpty(items)) {
      return null;
    }
    Map<String, Function> functionMap = new HashMap<String, Function>(items.size());

    for (FormulaParameterItem item : items) {
      functionMap.put(item.getName(), item.getFunction());
    }
    return functionMap;
  }

  public static CommonIdResult buildCommonDescriptor(FormulaParameterContainer container) throws XParameterException,
    SegmentException {
    long t1 = System.currentTimeMillis();

    List<FormulaParameterItem> items = container.getItems();
    List<FormulaQueryDescriptor> normalConnectors, totalConnectors, naturalConnectors;

    String id = container.getId();
    String projectId = container.getProjectId();
    Interval interval = container.getInterval();
    String beginDate = container.getBeginDate();
    String endDate = container.getEndDate();
    String formula = container.getFormula();
    Map<String, Function> functionMap = extractFunctionMap(container);

    Map<String, CommonItemResult> itemResultMap = new HashMap<String, CommonItemResult>(items.size());
    CommonIdResult idr = new CommonIdResult(id, formula, functionMap, itemResultMap);
    CommonItemResult cir;

    String name, event, realBeginDate, realEndDate, segment;
    Filter filter;
    Integer nd, ndo;
    Function function;

    DateTruncateLevel dateTruncateLevel;
    float intervalFloat = interval.getDays();
    double samplingRate;
    Date truncateTargetDate = (intervalFloat < 1 ? today() : yesterday());

    boolean ignoreHandlers = intervalFloat < 1;

    List<BeginEndDatePair> datePairs;
    BeginEndDatePair tmpPair;
    FormulaQueryDescriptor fqd;
    try {
      datePairs = split2Pairs(beginDate, endDate, interval);
    } catch (ParseException e) {
      throw new XParameterException(e);
    }
    int dataPairSize = datePairs.size();

    // 查询的时间跨度大于1个时间跨度
    boolean gt1 = dataPairSize > 1;

    // 是否有Segment
    boolean hasSegment;

    // 是否需要进行平均值计算
    boolean averageMetric;

    // 是否有可用的NumberOfDay或NumberOfDayOrigin
    boolean hasUsefulNDorNDO;

    // 是否可以累加结果得出汇总
    boolean accumulativeMetric;

    int booleanInteger;
    AggregationPolicy tap, nap;

    Set<Object> totalKeyIntersection = null;
    Set<Object> naturalKeyIntersection = null;

    String[] splitSegments;

    CommonItemResult proxyCommonItemResult;
    List<CommonItemResult> commonItemResults;

    List<SegmentHandler> handlers;
    for (FormulaParameterItem item : items) {
      name = item.getName();
      event = item.getEvent();
      segment = item.getSegment();
      filter = item.getFilter();
      nd = item.getCoverRange();
      ndo = item.getCoverRangeOrigin();
      function = item.getFunction();
      samplingRate = resolveSamplingRateByEvent(event, function);

      hasSegment = !(Strings.isNullOrEmpty(segment) || TOTAL_USER.equals(segment)
      );
      averageMetric = isAverage(nd, ndo);
      accumulativeMetric = isAccumulative(projectId, interval, item);

      booleanInteger = booleans2Int(gt1, hasSegment, averageMetric, accumulativeMetric);

      hasUsefulNDorNDO = hasUsefulNDorNDO(nd, ndo);

      handlers = createHandlers(segment);

      // 拆分带数组的Segment
      if (NEW_SEGMENT) {
        splitSegments = SegmentSeparator.generateNewSegments(segment);
      } else {
        splitSegments = SegmentSplitter.generateNewSegments(segment);
      }
      commonItemResults = new ArrayList<CommonItemResult>(splitSegments.length);
      // 生成Total所必须的Descriptor
      tap = parseTotalSummaryPolicy(booleanInteger, interval);
      if (tap.needCheckIntersection() && totalKeyIntersection == null) {
        totalKeyIntersection = new HashSet<Object>();
      }
      // 提前在全局生成生成NATURAL策略, 以及准备好计算交集用的集合
      nap = parseNaturalSummaryPolicy(booleanInteger, interval);
      if (nap.needCheckIntersection() && naturalKeyIntersection == null) {
        naturalKeyIntersection = new HashSet<Object>();
      }
      dateTruncateLevel = getDateTruncateLeve(averageMetric, accumulativeMetric);
      for (String singleSplitSegment : splitSegments) {
        normalConnectors = new ArrayList<FormulaQueryDescriptor>(dataPairSize);
        for (BeginEndDatePair pair : datePairs) {
          beginDate = pair.getBeginDate();
          endDate = pair.getEndDate();
          if (intervalFloat < 1) {
            fqd = new CommonFormulaQueryDescriptor(projectId, beginDate, endDate, event, singleSplitSegment, filter,
                                                   samplingRate, interval, NORMAL);
          } else if (hasUsefulNDorNDO) {
            tmpPair = getRealBeginEndDatePair(beginDate, endDate, nd, ndo);
            realBeginDate = tmpPair.getBeginDate();
            realEndDate = tmpPair.getEndDate();
            fqd = new CommonFormulaQueryDescriptor(projectId, realBeginDate, realEndDate, event, singleSplitSegment,
                                                   filter, samplingRate, beginDate, endDate, PERIOD, NORMAL);
          } else {
            fqd = new CommonFormulaQueryDescriptor(projectId, beginDate, endDate, event, singleSplitSegment, filter,
                                                   samplingRate, PERIOD, NORMAL);
          }
          fqd.addFunction(function);
          normalConnectors.add(fqd);
          putMonitorInfo(MI_DESCRIPTOR_BUILD);
        }
        truncateDate(normalConnectors, truncateTargetDate, dateTruncateLevel);
        // 处理Segment
        if (NEW_SEGMENT) {
          evaluate(normalConnectors);
        } else {
          handleSegment(handlers, normalConnectors, ignoreHandlers);
        }

        if (QUERY.equals(tap)) {
          totalConnectors = new ArrayList<FormulaQueryDescriptor>(1);
          fqd = new CommonFormulaQueryDescriptor(projectId, container.getBeginDate(), container.getEndDate(), event,
                                                 TOTAL_USER, filter, samplingRate, PERIOD, TOTAL);
          fqd.addFunction(function);
          totalConnectors.add(fqd);
          putMonitorInfo(MI_DESCRIPTOR_BUILD);
        } else if (SAME_AS_QUERY_EXTEND.equals(tap) || ACCUMULATION_EXTEND.equals(tap)) {
          totalConnectors = new ArrayList<FormulaQueryDescriptor>(dataPairSize);
          for (BeginEndDatePair pair : datePairs) {
            beginDate = pair.getBeginDate();
            endDate = pair.getEndDate();
            // 如果有NumberOfDay和NumberOfDayOrigin, 则使用Pair的日期
            if (hasUsefulNDorNDO) {
              tmpPair = getRealBeginEndDatePair(beginDate, endDate, nd, ndo);
              realBeginDate = tmpPair.getBeginDate();
              realEndDate = tmpPair.getEndDate();
              fqd = new CommonFormulaQueryDescriptor(projectId, realBeginDate, realEndDate, event, TOTAL_USER, filter,
                                                     samplingRate, beginDate, endDate, PERIOD, NORMAL);
            } else {
              fqd = new CommonFormulaQueryDescriptor(projectId, beginDate, endDate, event, TOTAL_USER, filter,
                                                     samplingRate, PERIOD, NORMAL);
            }
            fqd.addFunction(function);
            totalConnectors.add(fqd);
            putMonitorInfo(MI_DESCRIPTOR_BUILD);
          }
        } else if (AVERAGE_EXTEND.equals(tap)) {
          totalConnectors = new ArrayList<FormulaQueryDescriptor>(datePairs.size());
          for (BeginEndDatePair pair : datePairs) {
            beginDate = pair.getBeginDate();
            endDate = pair.getEndDate();
            if (hasUsefulNDorNDO) {
              tmpPair = getRealBeginEndDatePair(beginDate, endDate, nd, ndo);
              realBeginDate = tmpPair.getBeginDate();
              realEndDate = tmpPair.getEndDate();
              fqd = new CommonFormulaQueryDescriptor(projectId, realBeginDate, realEndDate, event, TOTAL_USER, filter,
                                                     samplingRate, beginDate, endDate, PERIOD, NORMAL);
            } else {
              fqd = new CommonFormulaQueryDescriptor(projectId, beginDate, endDate, event, TOTAL_USER, filter,
                                                     samplingRate, PERIOD, NORMAL);
            }
            fqd.addFunction(function);
            totalConnectors.add(fqd);
            putMonitorInfo(MI_DESCRIPTOR_BUILD);
          }
        } else {
          totalConnectors = null;
        }
        truncateDate(totalConnectors, truncateTargetDate, dateTruncateLevel);
        if (QUERY.equals(nap)) {
          naturalConnectors = new ArrayList<FormulaQueryDescriptor>(1);
          fqd = new CommonFormulaQueryDescriptor(projectId, container.getBeginDate(), container.getEndDate(), event,
                                                 singleSplitSegment, filter, samplingRate, PERIOD, NATURAL);
          fqd.addFunction(function);
          naturalConnectors.add(fqd);
          putMonitorInfo(MI_DESCRIPTOR_BUILD);
        } else if (SAME_AS_QUERY_EXTEND.equals(nap)) {
          naturalConnectors = new ArrayList<FormulaQueryDescriptor>(dataPairSize);
          for (BeginEndDatePair pair : datePairs) {
            beginDate = pair.getBeginDate();
            endDate = pair.getEndDate();
            if (hasUsefulNDorNDO) {
              tmpPair = getRealBeginEndDatePair(beginDate, endDate, nd, ndo);
              realBeginDate = tmpPair.getBeginDate();
              realEndDate = tmpPair.getEndDate();
              fqd = new CommonFormulaQueryDescriptor(projectId, realBeginDate, realEndDate, event, TOTAL_USER, filter,
                                                     samplingRate, beginDate, endDate, PERIOD, NORMAL);
            } else {
              fqd = new CommonFormulaQueryDescriptor(projectId, beginDate, endDate, event, singleSplitSegment, filter,
                                                     samplingRate, PERIOD, NORMAL);
            }
            fqd.addFunction(function);
            naturalConnectors.add(fqd);
            putMonitorInfo(MI_DESCRIPTOR_BUILD);
          }
        } else {
          naturalConnectors = null;
        }
        truncateDate(naturalConnectors, truncateTargetDate, dateTruncateLevel);
        // 处理Segment
        if (NEW_SEGMENT) {
          evaluate(naturalConnectors);
        } else {
          handleSegment(handlers, naturalConnectors, false);
        }

        cir = new CommonItemResult(name, normalConnectors, totalConnectors, naturalConnectors, tap, nap,
                                   totalKeyIntersection, naturalKeyIntersection);

        commonItemResults.add(cir);
      }

      proxyCommonItemResult = new CommonItemResultGroup(name, tap, nap, commonItemResults);

      itemResultMap.put(name, proxyCommonItemResult);
    }
    long t2 = System.currentTimeMillis();
    putMonitorInfo(new MonitorInfo(MI_STR_TIME_USE_BUILD_DESCRIPTOR, t2 - t1));
    idr.init(itemResultMap.size());
    return idr;
  }

  /**
   * Annotated by Z J Wu @ 2013年1月31日. 有关阶段日期逻辑是严格还是宽松, 情况有变. 在2013年1月31日认为, 曾经不可接受的, 使用宽松截断日期导致留存率平均数乱套的问题, 已经可以接受.
   * 因为严格的截断策略, 会导致ROI7等指标完全没数. 为了出数, 现在所有的日期截断逻辑, 均使用宽松截断策略, 不论是否有NumberOfDay. COMMON和GROUP皆是如此. 为了防止以后逻辑调整, 保留代码,
   * 但可以看到, 在ModelUtils中的getDateTruncateLevel方法, 已经直接返回宽松. 而GroupBy, 则认为latestAvailableGroupByDate变量就是当天/昨天(根据是否是HOUR,
   * MIN5还是PEROID决定), 代码修改情况本类的两个TODO标签.
   *
   * @param container
   * @return
   * @throws XParameterException
   * @throws SegmentException
   */
  public static GroupByIdResult buildGroupByDescriptor(FormulaParameterContainer container) throws XParameterException,
    SegmentException {
    long t1 = System.currentTimeMillis();

    List<FormulaParameterItem> items = container.getItems();
    List<FormulaQueryDescriptor> descriptors;
    FormulaQueryDescriptor descriptor;
    String id = container.getId();
    String projectId = container.getProjectId();
    String beginDate = container.getBeginDate();
    Interval interval = container.getInterval();
    String endDate = container.getEndDate();
    String formula = container.getFormula();
    String slicePattern = container.getSlicePattern();
    SliceType sliceType = container.getSliceType();

    Map<String, Function> functionMap = extractFunctionMap(container);
    String event = null;
    String name, segment, groupBy;
    Filter filter;
    Integer nd, ndo;
    Function function = null;
    GroupByType groupByType;
    DateTruncateType dtt;
    AggregationPolicy ap;

    double samplingRate = resolveSamplingRateByEvent(event, function);
    List<BeginEndDatePair> pairs = null;

    Map<String, GroupByItemResult> itemResultMap = new HashMap<String, GroupByItemResult>(items.size());

    GroupByItemResult groupByItemResult;

    BeginEndDatePair tmpPair, p;
    String latestAvailableGroupByDate = null;
    String singleDate = null;
    boolean hasAvailableDate, averageMetric;
    Date targetTrimDate = interval.getDays() < 1 ? today() : yesterday();
    boolean accumulativeMetric = false;

    DateTruncateLevel dateTruncateLevel;
    for (FormulaParameterItem item : items) {
      hasAvailableDate = false;
      nd = item.getCoverRange();
      ndo = item.getCoverRangeOrigin();

      averageMetric = isAverage(nd, ndo);

      // FIXME 2013-04-10
      // 由于有人查询跨度超长的GroupBy, 导致可累加的项达到上百个, 大大增加了
      // 后台的压力, 因此暂时去掉判断可累加行的判断
      // 不使用累加会导致新增用户出数据太慢, 但暂时先这样做
      // accumulativeMetric = isAccumulative(projectId, interval, item);
      // if (!averageMetric && !accumulativeMetric) {
      // continue;
      // }
      if (!averageMetric) {
        continue;
      }
      // 一组Item中, 如果有一个处于不可计算状态, 那别的也别继续试算了, 因此这里要停止
      if (INTERNAL_NA.equals(latestAvailableGroupByDate)) {
        break;
      }
      if (CollectionUtils.isEmpty(pairs)) {
        try {
          pairs = split2Pairs(beginDate, endDate, DAY);
        } catch (ParseException e) {
          throw new XParameterException(e);
        }
      }

      for (int i = pairs.size() - 1; i >= 0; i--) {
        p = pairs.get(i);
        singleDate = p.getBeginDate();
        tmpPair = getRealBeginEndDatePair(singleDate, singleDate, nd, ndo);
        dtt = truncateDate(tmpPair.getBeginDate(), tmpPair.getEndDate(), targetTrimDate, STRICTLY);
        if (PASS.equals(dtt)) {
          hasAvailableDate = true;
          break;
        }
      }
      if (hasAvailableDate) {
        try {
          if (latestAvailableGroupByDate == null) {
            latestAvailableGroupByDate = singleDate;
          } else if (before(singleDate, latestAvailableGroupByDate)) {
            latestAvailableGroupByDate = singleDate;
          }
        } catch (ParseException e) {
          throw new XParameterException(e);
        }
      } else {
        latestAvailableGroupByDate = INTERNAL_NA;
      }
    }
    GroupByFormulaParameterItem gbfpi;
    String[] splitSegments;
    List<GroupByItemResult> groupByItemResults;
    GroupByItemResult groupByItemResultProxy;

    List<SegmentHandler> handlers = null;

    // ======================================
    // TODO 参看方法注解

    // try {
    // String targetTrimDateString = date2Short(targetTrimDate);
    // latestAvailableGroupByDate = after(endDate, targetTrimDateString) ?
    // targetTrimDateString
    // : endDate;
    // } catch (ParseException e1) {
    // throw new XParameterException(e1);
    // }
    latestAvailableGroupByDate = endDate;
    // ======================================

    // 聚合计算-统一化聚合口径-killed的fqd检查
    Set<Object> killedFqdSet = new HashSet<Object>();
    // 聚合计算-统一化聚合口径-passed的fqd检查
    Set<Object> missedFqdSet = new HashSet<Object>();
    for (FormulaParameterItem item : items) {
      name = item.getName();
      event = item.getEvent();
      segment = item.getSegment();
      filter = item.getFilter();
      function = item.getFunction();
      nd = item.getCoverRange();
      ndo = item.getCoverRangeOrigin();
      gbfpi = (GroupByFormulaParameterItem) item;
      groupBy = gbfpi.getGroupBy();
      groupByType = gbfpi.getGroupByType();

      if (NEW_SEGMENT) {
        splitSegments = SegmentSeparator.generateNewSegments(segment);
      } else {
        handlers = createHandlers(segment);
        splitSegments = SegmentSplitter.generateNewSegments(segment);
      }

      groupByItemResults = new ArrayList<GroupByItemResult>(splitSegments.length);

      averageMetric = isAverage(nd, ndo);
      // 三种认为可以直接累加结果的情形
      // 1.Function为Count
      // 2.Function为Sum
      // 3.Function为UserNum, 但是其Segment是不变Segment

      // FIXME 2013-04-10

      if (BATCH_GROUPBY) {
//        由于有人查询跨度超长的GroupBy, 导致可累加的项达到上百个, 大大增加了
//        后台的压力, 因此暂时去掉判断可累加行的判断
//        不使用累加会导致新增用户出数据太慢, 但暂时先这样做
        accumulativeMetric = isAccumulative(projectId, interval, item);
        if (averageMetric) {
          ap = AVERAGE;
        } else if (accumulativeMetric) {
          ap = ACCUMULATION;
        } else {
          ap = QUERY;
        }
      } else {
        if (averageMetric) {
          ap = AVERAGE;
        } else {
          ap = QUERY;
        }
      }

      dateTruncateLevel = getDateTruncateLeve(averageMetric, accumulativeMetric);

      for (String splitSegmentPart : splitSegments) {
        if (averageMetric) {
          if (INTERNAL_NA.equals(latestAvailableGroupByDate)) {
            descriptors = new ArrayList<FormulaQueryDescriptor>(1);
            descriptor = new GroupByFormulaQueryDescriptor(projectId, beginDate, endDate, event, splitSegmentPart,
                                                           filter, samplingRate, groupBy, groupByType);
            descriptor.setDateTruncateType(KILL);
            descriptors.add(descriptor);
            putMonitorInfo(MI_DESCRIPTOR_BUILD);
          } else {
            // FIXME 重新做一下PAIR, 这里可优化, 可以使用上面的pair
            try {
              pairs = split2Pairs(beginDate, latestAvailableGroupByDate, DAY);
            } catch (ParseException e) {
              throw new XParameterException(e);
            }
            int pairSize = pairs.size();
            descriptors = new ArrayList<FormulaQueryDescriptor>(pairSize);
            for (int i = 0; i < pairSize; i++) {
              p = pairs.get(i);
              tmpPair = getRealBeginEndDatePair(p.getBeginDate(), p.getEndDate(), nd, ndo);
              descriptor = new GroupByFormulaQueryDescriptor(projectId, tmpPair.getBeginDate(), tmpPair.getEndDate(),
                                                             event, splitSegmentPart, filter, samplingRate,
                                                             p.getBeginDate(), p.getEndDate(), groupBy, groupByType);
              descriptor.setDateTruncateType(PASS);

              // ======================================
              // TODO 参看方法注解
              truncateDate(descriptor, targetTrimDate, dateTruncateLevel);
              // ======================================

              descriptor.addFunction(function);
              descriptors.add(descriptor);
              // 处理Segment
              if (NEW_SEGMENT) {
                evaluate(descriptor);
              } else {
                handleSegment(handlers, descriptor, false);
              }
              putMonitorInfo(MI_DESCRIPTOR_BUILD);
            }
          }
          // FIXME 2013-04-10
          // 由于有人查询跨度超长的GroupBy, 导致可累加的项达到上百个, 大大增加了
          // 后台的压力, 因此暂时去掉判断可累加行的判断
          // 不使用累加会导致新增用户出数据太慢, 但暂时先这样做
        } else if (BATCH_GROUPBY && accumulativeMetric) {
          // FIXME 重新做一下PAIR, 这里可优化, 可以使用上面的pair
          try {
            pairs = split2Pairs(beginDate, endDate, DAY);
          } catch (ParseException e) {
            throw new XParameterException(e);
          }
          int pairSize = pairs.size();
          descriptors = new ArrayList<FormulaQueryDescriptor>(pairSize);
          for (int i = 0; i < pairSize; i++) {
            p = pairs.get(i);
            tmpPair = getRealBeginEndDatePair(p.getBeginDate(), p.getEndDate(), nd, ndo);
            descriptor = new GroupByFormulaQueryDescriptor(projectId, tmpPair.getBeginDate(), tmpPair.getEndDate(),
                                                           event, splitSegmentPart, filter, samplingRate,
                                                           p.getBeginDate(), p.getEndDate(), groupBy, groupByType);
            truncateDate(descriptor, targetTrimDate, dateTruncateLevel);
            descriptor.addFunction(function);
            descriptors.add(descriptor);
            // 处理Segment
            if (NEW_SEGMENT) {
              evaluate(descriptor);
            } else {
              handleSegment(handlers, descriptor, false);
            }
            putMonitorInfo(MI_DESCRIPTOR_BUILD);
          }
        } else {
          descriptors = new ArrayList<FormulaQueryDescriptor>(1);
          descriptor = new GroupByFormulaQueryDescriptor(projectId, beginDate, endDate, event, splitSegmentPart, filter,
                                                         samplingRate, groupBy, groupByType);
          truncateDate(descriptor, targetTrimDate, dateTruncateLevel);
          descriptor.addFunction(function);
          // 处理Segment
          if (NEW_SEGMENT) {
            evaluate(descriptor);
          } else {
            handleSegment(handlers, descriptor, false);
          }

          descriptors.add(descriptor);
          putMonitorInfo(MI_DESCRIPTOR_BUILD);
        }

        groupByItemResult = new GroupByItemResult(name, descriptors, ap, killedFqdSet, missedFqdSet);
        groupByItemResults.add(groupByItemResult);
      }

      groupByItemResultProxy = new GroupByItemResultGroup(name, groupByItemResults);
      itemResultMap.put(name, groupByItemResultProxy);
    }
    GroupByIdResult idResult;
    String description;
    if (Strings.isNullOrEmpty(latestAvailableGroupByDate)) {
      description = beginDate + " - " + endDate;
    } else if (INTERNAL_NA.equals(latestAvailableGroupByDate)) {
      description = INTERNAL_NA;
    } else {
      description = beginDate + " - " + latestAvailableGroupByDate;
    }
    idResult = new GroupByIdResult(id, formula, functionMap, slicePattern, sliceType, description, itemResultMap,
                                   killedFqdSet, missedFqdSet);
    long t2 = System.currentTimeMillis();
    putMonitorInfo(new MonitorInfo(MI_STR_TIME_USE_BUILD_DESCRIPTOR, t2 - t1));
    return idResult;
  }

  public static void main(String[] args) throws XParameterException, SegmentException, UserPropertyException {
    String id = "abc001";
    String f = "x";
    String e = "visit.*";
    String projectId = "tencent-18894";
    String beginDate = "2012-12-05";
    String endDate = "2012-12-09";
    String s = "{\"grade\":10}";
    Interval i = Interval.DAY;

    FormulaParameterContainer c01 = new FormulaParameterContainer(id, projectId, beginDate, endDate, i, f,
                                                                  new ArrayList<FormulaParameterItem>(), null, COMMON);
    c01.getItems().add(new CommonFormulaParameterItem("x", e, s, null, USER_NUM, -5, -5));
    // c01.getItems().add(
    // new CommonFormulaParameterItem("y", e, s, null, COUNT, null,
    // null));
    c01.init();

    System.out.println(buildCommonDescriptor(c01));
    System.out.println("===================================");
    FormulaParameterContainer c02 = new FormulaParameterContainer(id, projectId, beginDate, endDate, i, f,
                                                                  new ArrayList<FormulaParameterItem>(), null, GROUP);
    c02.getItems().add(new GroupByFormulaParameterItem("x", e, s, null, USER_NUM, -3, -3, "grade", USER_PROPERTIES));
    c02.getItems().add(new GroupByFormulaParameterItem("y", e, s, null, USER_NUM, -4, -4, "grade", USER_PROPERTIES));
    c02.init();
    System.out.println(buildGroupByDescriptor(c02));
  }

}
