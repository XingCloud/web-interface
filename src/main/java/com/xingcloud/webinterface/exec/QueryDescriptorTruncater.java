package com.xingcloud.webinterface.exec;

import static com.xingcloud.basic.utils.DateUtils.date2Short;
import static com.xingcloud.basic.utils.DateUtils.short2Date;
import static com.xingcloud.webinterface.enums.DateTruncateLevel.STRICTLY;
import static com.xingcloud.webinterface.enums.DateTruncateType.KILL;
import static com.xingcloud.webinterface.enums.DateTruncateType.PASS;
import static com.xingcloud.webinterface.enums.DateTruncateType.TRIM;

import com.xingcloud.webinterface.enums.DateTruncateLevel;
import com.xingcloud.webinterface.enums.DateTruncateType;
import com.xingcloud.webinterface.exception.XParameterException;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import org.apache.commons.collections.CollectionUtils;

import java.util.Collection;
import java.util.Date;

public class QueryDescriptorTruncater {
  public static DateTruncateType truncateDate(String beginDate, String endDate, Date targetDate,
                                              DateTruncateLevel dateTruncateLevel) throws XParameterException {
    Date d1, d2;
    try {
      d1 = short2Date(beginDate);
      d2 = short2Date(endDate);

      if (STRICTLY.equals(dateTruncateLevel)) {
        if (d1.after(targetDate) || d2.after(targetDate)) {
          return KILL;
        } else {
          return PASS;
        }
      } else {
        if (d1.after(targetDate) && d2.after(targetDate)) {
          return KILL;
        } else if (!d1.after(targetDate) && d2.after(targetDate)) {
          return TRIM;
        } else {
          return PASS;
        }
      }
    } catch (Exception e) {
      throw new XParameterException(e);
    }
  }

  public static void truncateDate(Collection<FormulaQueryDescriptor> distinctDescriptors, Date targetDate,
                                  DateTruncateLevel dateTruncateLevel) throws XParameterException {
    if (CollectionUtils.isEmpty(distinctDescriptors)) {
      return;
    }
    for (FormulaQueryDescriptor fqd : distinctDescriptors) {
      truncateDate(fqd, targetDate, dateTruncateLevel);
    }
  }

  // TODO 截断策略本应是简单的, 但由于目前系统对当天查询的实时性不足够好, 为了减轻压力, 临时使用这种策略,
  // 即截断到昨天,
  // 这里的代码根据以后的情况决定, 如果系统需要多样性的阶段策略, 就优化这里, 如果不需要, 就还原以前的老策略
  public static void truncateDate(FormulaQueryDescriptor descriptor, Date targetDate,
                                  DateTruncateLevel dateTruncateLevel) throws XParameterException {
    if (descriptor == null) {
      return;
    }
    Date d1, d2;
    try {
      d1 = short2Date(descriptor.getRealBeginDate());
      d2 = short2Date(descriptor.getRealEndDate());
      if (STRICTLY.equals(dateTruncateLevel)) {
        if (d1.after(targetDate) || d2.after(targetDate)) {
          descriptor.setDateTruncateType(KILL);
        } else {
          descriptor.setDateTruncateType(PASS);
        }
      } else {
        if (d1.after(targetDate) && d2.after(targetDate)) {
          descriptor.setDateTruncateType(KILL);
        } else if (!d1.after(targetDate) && d2.after(targetDate)) {
          descriptor.setDateTruncateType(TRIM);
          descriptor.setRealEndDate(date2Short(targetDate));
        } else {
          descriptor.setDateTruncateType(PASS);
        }
      }
    } catch (Exception e) {
      throw new XParameterException(e);
    }
  }
}
