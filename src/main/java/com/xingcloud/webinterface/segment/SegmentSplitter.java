package com.xingcloud.webinterface.segment;

import static com.xingcloud.webinterface.utils.CartesianProduct.travel;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.DEFAULT_GSON_PLAIN;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.TOTAL_USER;

import com.google.common.base.Strings;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import com.xingcloud.webinterface.exception.SegmentException;
import com.xingcloud.webinterface.model.Pair;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

public class SegmentSplitter {

  private static final Logger LOGGER = Logger.getLogger(SegmentSplitter.class);

  public static SegmentContainer splitSegment(String segment) throws SegmentException {
    if (Strings.isNullOrEmpty(segment) || TOTAL_USER.equals(segment)) {
      return null;
    }

    Map<String, List<Object>> segmentMap = null;

    Map<Object, Object> tmpSegmentMap = ((DBObject) JSON.parse(segment)).toMap();

    String fieldKey = null;
    Object fieldValue;

    Iterator<Entry<Object, Object>> it = tmpSegmentMap.entrySet().iterator();

    Entry<Object, Object> entryObject;
    BasicDBList dbl;
    while (it.hasNext()) {
      entryObject = it.next();
      if (entryObject == null) {
        throw new SegmentException("Part of segment is null - " + segment);
      }
      if (!(entryObject.getKey() instanceof String)) {
        throw new SegmentException("Cannot parse segment item because its name is not string - " + fieldKey);
      }
      fieldKey = (String) entryObject.getKey();
      fieldValue = entryObject.getValue();

      // 处理in关键句
      if (fieldValue instanceof BasicDBList) {
        dbl = (BasicDBList) fieldValue;
        it.remove();
        if (dbl.isEmpty()) {
          continue;
        }

        if (segmentMap == null) {
          segmentMap = new HashMap<String, List<Object>>();
        }
        segmentMap.put(entryObject.getKey().toString(), (List<Object>) fieldValue);
      }
    }
    SegmentContainer segmentContainer =
      MapUtils.isEmpty(segmentMap) ? null : new SegmentContainer(tmpSegmentMap, segmentMap);
    return segmentContainer;
  }

  public static String[] generateNewSegments(String segment) throws SegmentException {
    Object[] returnResults;
    if (Strings.isNullOrEmpty(segment) || TOTAL_USER.equals(segment)) {
//      LOGGER.info("[SEGMENT] - " + TOTAL_USER + ", ignore split.");
      return new String[]{TOTAL_USER};
    }
    SegmentContainer segmentContainer = splitSegment(segment);
    if (segmentContainer == null) {
//      LOGGER.info(
//          "[SEGMENT] - SegmentPart doesn't contain any splittable content, ignore split - " + segment);
      return new String[]{segment};
    }
    Map<String, List<Object>> splitSegmentMap = segmentContainer.getSplittedSegmentPart();
    Map<Object, Object> fixedSegmentMap = segmentContainer.getFixedSegmentMap();
    Method m;
    try {
      m = SegmentSplitter.class.getMethod("putNewSegment", Pair[].class, Map.class);
      returnResults = travel(splitSegmentMap, m, fixedSegmentMap);
    } catch (Exception e) {
      throw new SegmentException(e);
    }
    if (ArrayUtils.isEmpty(returnResults)) {
      throw new SegmentException("How can a segment split to empty? There must be something wrong.");
    }

    String[] newSegmentArray = new String[returnResults.length];
    for (int i = 0; i < returnResults.length; i++) {
      newSegmentArray[i] = returnResults[i].toString();
    }
    return newSegmentArray;
  }

  public static String putNewSegment(Pair[] pairs, Map<Object, Object> fixedSegmentMap) {
    Map<Object, Object> sortedMap;
    if (ArrayUtils.isEmpty(pairs)) {
      sortedMap = new TreeMap<Object, Object>(fixedSegmentMap);
      return DEFAULT_GSON_PLAIN.toJson(sortedMap);
    }
    sortedMap = new TreeMap<Object, Object>(fixedSegmentMap);
    for (Pair pair : pairs) {
      sortedMap.put(pair.getK(), pair.getV());
    }
    return DEFAULT_GSON_PLAIN.toJson(sortedMap);
  }
}
