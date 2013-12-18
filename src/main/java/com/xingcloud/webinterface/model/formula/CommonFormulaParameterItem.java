package com.xingcloud.webinterface.model.formula;

import static com.xingcloud.webinterface.enums.Function.COUNT;
import static com.xingcloud.webinterface.enums.Function.SUM;
import static com.xingcloud.webinterface.enums.Function.USER_NUM;
import static com.xingcloud.webinterface.utils.ModelUtils.hasNDorNDO;
import static com.xingcloud.webinterface.utils.WebInterfaceCommonUtils.booleans2Int;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.MIN_HOUR_SUMMARY_POLICY_ARR_NATURAL;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.MIN_HOUR_SUMMARY_POLICY_ARR_TOTAL;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.PERIOD_SUMMARY_POLICY_ARR_NATURAL;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.PERIOD_SUMMARY_POLICY_ARR_TOTAL;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.TOTAL_USER;

import com.google.common.base.Strings;
import com.xingcloud.webinterface.enums.AggregationPolicy;
import com.xingcloud.webinterface.enums.Function;
import com.xingcloud.webinterface.enums.Interval;
import com.xingcloud.webinterface.exception.NumberOfDayException;
import com.xingcloud.webinterface.exception.XParameterException;
import com.xingcloud.webinterface.model.BeginEndDatePair;
import com.xingcloud.webinterface.model.Filter;
import com.xingcloud.webinterface.syncmetric.model.AbstractSync;
import com.xingcloud.webinterface.syncmetric.model.CommonSync;
import com.xingcloud.webinterface.utils.DateSplitter;
import org.apache.commons.collections.CollectionUtils;

import java.text.ParseException;
import java.util.List;

public class CommonFormulaParameterItem extends FormulaParameterItem {

  private List<BeginEndDatePair> datePairs;

  private AggregationPolicy totalSummaryPolicy;

  private AggregationPolicy naturalSummaryPolicy;

  private String scale;

  public CommonFormulaParameterItem() {
    super();
  }

  public CommonFormulaParameterItem(String name, String event, String segment, Filter filter, Function function,
                                    Integer coverRangeOrigin, Integer coverRange, String scale) {
    super(name, event, segment, filter, function, coverRangeOrigin, coverRange);
  }

  public AggregationPolicy getTotalSummaryPolicy() {
    return totalSummaryPolicy;
  }

  public AggregationPolicy getNaturalSummaryPolicy() {
    return naturalSummaryPolicy;
  }

  public String getScale() {
    return scale;
  }

  public void parseSummaryPolicy(Interval interval) {
    Function function = getFunction();
    String segment = getSegment();
    Integer nd = getCoverRange();
    Integer ndo = getCoverRangeOrigin();

    float intervalFloat = interval.getDays();

    // 查询的时间跨度大于1个时间跨度
    boolean gt1 = false;
    if (datePairs.size() > 1) {
      gt1 = true;
    }

    // 是否有Segment
    boolean hasSegment = true;
    if (Strings.isNullOrEmpty(segment) || TOTAL_USER.equals(segment)) {
      hasSegment = false;
    }

    // 是否需要进行平均值计算
    boolean hasNumberOfDay = true;
    if (nd == null && ndo == null) {
      hasNumberOfDay = false;
    }

    // 是否可以累加结果得出汇总
    boolean accu = true;
    if (USER_NUM.equals(function)) {
      accu = false;
    }

    int booleanInteger = booleans2Int(gt1, hasSegment, hasNumberOfDay, accu);
    // 查询小时/5分钟的情况
    if (intervalFloat < 1) {
      this.totalSummaryPolicy = MIN_HOUR_SUMMARY_POLICY_ARR_TOTAL[booleanInteger];
      this.naturalSummaryPolicy = MIN_HOUR_SUMMARY_POLICY_ARR_NATURAL[booleanInteger];
    }
    // 查询天/周/月的情况
    else {
      this.totalSummaryPolicy = PERIOD_SUMMARY_POLICY_ARR_TOTAL[booleanInteger];
      this.naturalSummaryPolicy = PERIOD_SUMMARY_POLICY_ARR_NATURAL[booleanInteger];
    }
  }

  public void init(FormulaParameterContainer container) throws XParameterException {
    Interval interval = container.getInterval();
    super.init(container);
    String beginDate = container.getBeginDate();
    String endDate = container.getEndDate();
    if (!(Strings.isNullOrEmpty(beginDate) || Strings.isNullOrEmpty(endDate))) {
      List<String> dateSlice;
      dateSlice = DateSplitter.getQueryDateSlice(beginDate, endDate, container.getInterval());
      try {
        datePairs = DateSplitter.getBeginEndDatePairs(dateSlice, container.getInterval());
      } catch (ParseException e) {
        throw new XParameterException("Cannot parse begin-date and end-date pair by given parameters - " + dateSlice);
      }
    }
    if (CollectionUtils.isEmpty(datePairs)) {
      throw new XParameterException("Empty query date-pair set is not valid because it's useless.");
    }
    String segment = getSegment();
    if (Strings.isNullOrEmpty(segment)) {
      setSegment(TOTAL_USER);
    }
    parseSummaryPolicy(interval);
  }

  @Override
  public void validate(FormulaParameterContainer fpc) throws XParameterException, NumberOfDayException {
    super.validate(fpc);
    Interval interval = fpc.getInterval();
    Integer nd = getCoverRange();
    Integer ndo = getCoverRangeOrigin();
    if (interval.getDays() < 1) {
      if (hasNDorNDO(nd, ndo)) {
        throw new NumberOfDayException(
          "Number-of-day or number-of-day-origin cannot " + "have value when interval is not [DAY|WEEK|MONTH]," + " actural value is [" + interval + "]");
      }
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("CFPI(");
    sb.append(name);
    sb.append('.');
    sb.append(event);
    sb.append('.');
    sb.append(segment);
    sb.append('.');
    sb.append(filter);
    sb.append('.');
    sb.append(function);
    sb.append(".(ND.");
    sb.append(this.coverRange);
    sb.append(".NDO.");
    sb.append(this.coverRangeOrigin);
    sb.append(")");
    return sb.toString();
  }

  @Override
  public boolean canAccumulateTotalAndNatural() {
    Function function = getFunction();
    return COUNT.equals(function) || SUM.equals(function);
  }

  @Override
  public AbstractSync makeSync(FormulaParameterContainer container) {
    String projectId = container.getProjectId();
    Interval interval = container.getInterval();
    String event = getEvent();
    String segment = getSegment();
    Integer ndo = getCoverRangeOrigin();
    Integer nd = getCoverRange();
    AbstractSync sync = new CommonSync(projectId, event, segment, ndo, nd, null, interval);
    return sync;
  }

}
