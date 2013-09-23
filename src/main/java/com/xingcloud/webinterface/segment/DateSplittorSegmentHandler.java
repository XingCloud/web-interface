package com.xingcloud.webinterface.segment;

import static com.xingcloud.basic.utils.DateUtils.dateSubtraction;
import static com.xingcloud.webinterface.enums.Operator.GTE;
import static com.xingcloud.webinterface.enums.Operator.LTE;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.HANDLER_KEYWORD;

import com.mongodb.BasicDBObject;
import com.xingcloud.webinterface.annotation.JsonName;
import com.xingcloud.webinterface.exception.SegmentException;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;

import java.text.ParseException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

public class DateSplittorSegmentHandler extends AbstractSegmentHandler {

  @JsonName("offset")
  private Integer offset;

  public Integer getOffset() {
    return offset;
  }

  public void setOffset(Integer offset) {
    this.offset = offset;
  }

  @Override
  public String toString() {
    return "DateSplittorSegmentHandler [offset=" + offset + "]";
  }

  @Override
  public void handleSegment(BasicDBObject segmentDBObject, FormulaQueryDescriptor descriptor) throws SegmentException {

    check(segmentDBObject, descriptor);

    String beginDate = descriptor.getInputBeginDate();
    String endDate = descriptor.getInputEndDate();

    if (offset != null && offset != 0) {
      try {
        beginDate = dateSubtraction(beginDate, offset);
        endDate = dateSubtraction(endDate, offset);
      } catch (ParseException e) {
        throw new SegmentException(e);
      }
    }

    Object propertyValue = null;
    String handlerName = null;
    Map<Object, Object> map = null;

    Map<Object, Object> conditionMap = null;
    for (Entry<String, Object> entry : segmentDBObject.entrySet()) {
      propertyValue = entry.getValue();
      if (!(propertyValue instanceof Map)) {
        continue;
      }

      map = (Map) propertyValue;
      handlerName = (String) map.get(HANDLER_KEYWORD);
      if (handlerName == null || !DateSplittorSegmentHandler.class.getName().contains(handlerName)) {
        continue;
      }
      map.remove(HANDLER_KEYWORD);

      conditionMap = new TreeMap<Object, Object>();
      conditionMap.put(GTE.getMongoKeyword(), beginDate);
      conditionMap.put(LTE.getMongoKeyword(), endDate);
      entry.setValue(conditionMap);

    }
  }

}
