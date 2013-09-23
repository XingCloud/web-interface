package com.xingcloud.webinterface.segment;

import com.mongodb.BasicDBObject;
import com.xingcloud.webinterface.exception.SegmentException;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;

import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

public class SortingSegmentHandler extends AbstractSegmentHandler {

  @Override
  public void handleSegment(BasicDBObject segmentDBObject, FormulaQueryDescriptor descriptor) throws SegmentException {
    check(segmentDBObject, descriptor);

    Object propertyValue = null;
    Map<Object, Object> sortedMap = null;
    for (Entry<String, Object> entry : segmentDBObject.entrySet()) {
      propertyValue = entry.getValue();
      if (!(propertyValue instanceof BasicDBObject)) {
        continue;
      }
      sortedMap = new TreeMap<Object, Object>((BasicDBObject) propertyValue);
      entry.setValue(sortedMap);
    }
  }

}
