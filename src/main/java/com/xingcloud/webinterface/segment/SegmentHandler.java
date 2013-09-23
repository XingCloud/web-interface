package com.xingcloud.webinterface.segment;

import com.mongodb.BasicDBObject;
import com.xingcloud.webinterface.exception.SegmentException;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;

public interface SegmentHandler {
  public void handleSegment(BasicDBObject segmentDBObject,
                            FormulaQueryDescriptor descriptor) throws SegmentException;
}
