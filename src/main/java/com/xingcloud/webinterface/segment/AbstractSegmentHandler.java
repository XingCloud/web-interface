package com.xingcloud.webinterface.segment;

import com.mongodb.BasicDBObject;
import com.xingcloud.webinterface.exception.SegmentException;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import org.apache.commons.collections.MapUtils;

public abstract class AbstractSegmentHandler implements SegmentHandler {

  protected void check(BasicDBObject segmentDBObject, FormulaQueryDescriptor descriptor) throws SegmentException {
    if (descriptor == null) {
      throw new SegmentException("Cannot execute segment handler because descriptor is null.");
    }

    if (MapUtils.isEmpty(segmentDBObject)) {
      throw new SegmentException("Cannot execute segment handler because its DBObject is null.");
    }
  }

  @Override
  public void handleSegment(BasicDBObject segmentDBObject, FormulaQueryDescriptor descriptor) throws SegmentException {
    check(segmentDBObject, descriptor);
  }

  public String toString() {
    return this.getClass().getName();
  }

}
