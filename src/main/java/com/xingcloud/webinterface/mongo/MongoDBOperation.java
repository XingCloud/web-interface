package com.xingcloud.webinterface.mongo;

import static com.xingcloud.webinterface.enums.Levels.l0;
import static com.xingcloud.webinterface.enums.Levels.l1;
import static com.xingcloud.webinterface.enums.Levels.l2;
import static com.xingcloud.webinterface.enums.Levels.l3;
import static com.xingcloud.webinterface.enums.Levels.l4;
import static com.xingcloud.webinterface.enums.Levels.l5;
import static com.xingcloud.webinterface.utils.WebInterfaceCommonUtils.appendStringBuilder;
import static com.xingcloud.webinterface.utils.WebInterfaceCommonUtils.put2MapIgnoreEmpty;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.TBL_USER_PROPERTIES_PROJECT_ID;

import com.google.common.base.Strings;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.xingcloud.mongo.MongoDBManager;
import com.xingcloud.webinterface.cache.CacheContainer;
import com.xingcloud.webinterface.enums.CacheKey;
import com.xingcloud.webinterface.enums.QueryType;
import com.xingcloud.webinterface.enums.SegmentSyncType;
import com.xingcloud.webinterface.exception.MongodbException;
import com.xingcloud.webinterface.model.mongo.EventListQueryDescriptor;
import com.xingcloud.webinterface.syncmetric.model.AbstractSync;
import com.xingcloud.webinterface.syncmetric.model.CommonSync;
import com.xingcloud.webinterface.syncmetric.model.GroupBySync;
import com.xingcloud.webinterface.utils.WebInterfaceCommonUtils;
import com.xingcloud.webinterface.utils.WebInterfaceConstants;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class MongoDBOperation {
  private static final String metricColl = "metrics";
  private static final Logger LOGGER = Logger.getLogger(MongoDBOperation.class);
  private static MongoDBOperation m_instance;
  private final CacheManager cacheManager = CacheManager.create(MongoDBOperation.class.getResource("/ehcache.xml"));

  private MongoDBManager mongoDBManager = MongoDBManager.getInstance();

  private DB db;

  private MongoDBOperation() {
    db = this.mongoDBManager.getDB();
  }

  public static synchronized MongoDBOperation getInstance() {
    if (m_instance == null) {
      m_instance = new MongoDBOperation();
    }
    return m_instance;
  }

  public MongoDBManager getMongoDBManager() {
    return mongoDBManager;
  }

  public void saveSegmentFragments(String projectId, Map<String, String> fragments, SegmentSyncType syncType) throws
    MongodbException {
    if (fragments == null || fragments.isEmpty()) {
      LOGGER.error("Cannot save a null segment.");
      throw new MongodbException("Cannot save a null segment.");
    }
    LOGGER.info("Saving - " + syncType + " - " + projectId);
    DBObject condition = new BasicDBObject("projectId", projectId);
    DBObject valueMap = new BasicDBObject("projectId", projectId);

    DBCollection coll = db.getCollection("segments");
    DBObject existsRecord = coll.findOne(condition);

    if (existsRecord == null) {
      valueMap.put("segments", fragments);
    } else {
      DBObject existsSegments = (DBObject) existsRecord.get("segments");
      existsSegments.putAll(fragments);
      valueMap.put("segments", existsSegments);
    }
    DBObject value = new BasicDBObject("$set", valueMap);
    LOGGER.info("Condition - " + condition);
    LOGGER.info("Value - " + value);
    coll.update(condition, value, true, true);
    LOGGER.info("Saving metric to mongodb finished - " + projectId);
  }

  public void removeSegmentFragments(String projectId, Set<String> segmentIds) {
    LOGGER.info("Removing - " + projectId);
    DBObject condition = new BasicDBObject("projectId", projectId);
    DBCollection coll = db.getCollection("segments");
    DBObject existsRecord = coll.findOne(condition);
    if (existsRecord == null) {
      LOGGER.info("Removing failed - there is no project called " + projectId);
      return;
    }
    DBObject existsSegments = (DBObject) existsRecord.get("segments");
    Map<String, String> m = existsSegments.toMap();

    for (String segmentId : segmentIds) {
      if (m.containsKey(segmentId)) {
        m.remove(segmentId);
      }
    }

    DBObject valueMap = new BasicDBObject("project_id", projectId);
    valueMap.put("segments", m);
    DBObject value = new BasicDBObject("$set", valueMap);
    LOGGER.info("Condition - " + condition);
    coll.update(condition, value, true, true);
    LOGGER.info("Removing - " + segmentIds + " finished");
  }

  public void saveMetric(AbstractSync as) throws Exception {
    if (as == null) {
      LOGGER.error("Cannot save metric because sync object is null");
      throw new MongodbException("Cannot save metric because sync object is null");
    }

    LOGGER.info("Syncing - " + as);
    DBCollection coll = db.getCollection(metricColl);
    Map<String, Object> valueMap = as.toMap();

    if (as instanceof CommonSync) {
      valueMap.put("type", QueryType.COMMON.name());
    } else if (as instanceof GroupBySync) {
      valueMap.put("type", QueryType.GROUP.name());
      valueMap.put("group_by", ((GroupBySync) as).getGroupByMap());
    } else {
      LOGGER.error("Unsupported sync type.");
      throw new MongodbException("Unsupported sync type.");
    }
    DBObject condition = new BasicDBObject(valueMap);
    DBObject value = new BasicDBObject("$set", valueMap);

    LOGGER.info("Condition & Value - " + valueMap);
    coll.update(condition, value, true, true);
    LOGGER.info("Saving metric to mongodb finished - " + as);
  }

  public void removeMetric(AbstractSync as) throws Exception {
    if (as == null) {
      LOGGER.error("Cannot delete metric because sync object is null");
      throw new MongodbException("Cannot delete metric because sync object is null");
    }

    DBCollection coll = db.getCollection(metricColl);
    Map<String, Object> conditionMap = as.toMap();

    if (as instanceof CommonSync) {
      conditionMap.put("type", QueryType.COMMON.name());
    } else if (as instanceof GroupBySync) {
      conditionMap.put("type", QueryType.GROUP.name());
      conditionMap.put("group_by", ((GroupBySync) as).getGroupByMap());
    } else {
      LOGGER.error("Unsupported sync type.");
      return;
    }

    DBObject condition = new BasicDBObject(conditionMap);
    coll.remove(condition);
    LOGGER.info("Removed metric: " + condition);
  }

  public Set<String> getEventSet(String projectId) {
    if (Strings.isNullOrEmpty(projectId)) {
      return null;
    }
    Cache cache = cacheManager.getCache(CacheKey.ALL_EVENTS.name());
    if (cache == null) {
      cache = new Cache(new CacheConfiguration(CacheKey.ALL_EVENTS.name(), WebInterfaceConstants.CACHE_OBJ_NUM_5K)
                          .overflowToDisk(WebInterfaceConstants.CACHE_MEMORY)
                          .eternal(WebInterfaceConstants.CACHE_TRANSIENT)
                          .timeToIdleSeconds(WebInterfaceConstants.CACHE_DURATION_5MIN)
                          .timeToLiveSeconds(WebInterfaceConstants.CACHE_DURATION_10MIN));
      cacheManager.addCache(cache);
    }
    Element element = cache.get(projectId);
    if (element != null) {
      LOGGER.info("Using cache(" + cache.getName() + ")");
      return (Set<String>) element.getObjectValue();
    }
    LOGGER.info("Query");
    DBCollection coll = db.getCollection("events_list");
    Set<String> events = null;

    DBObject query = new BasicDBObject();
    query.put(TBL_USER_PROPERTIES_PROJECT_ID, projectId);
    long cnt = coll.count(query);
    DBCursor cursor = coll.find(query);
    events = new HashSet<String>((int) cnt);
    StringBuilder sb = null;
    while (cursor.hasNext()) {
      sb = new StringBuilder();
      sb.append(cursor.next().get(l0.name()));
      sb.append('.');
      sb.append(cursor.next().get(l1.name()));
      sb.append('.');
      sb.append(cursor.next().get(l2.name()));
      sb.append('.');
      sb.append(cursor.next().get(l3.name()));
      sb.append('.');
      sb.append(cursor.next().get(l4.name()));
      sb.append('.');
      sb.append(cursor.next().get(l5.name()));
      sb.append('.');
      events.add(sb.toString());
    }
    element = new Element(projectId, events);
    cache.put(element);
    return events;
  }

  public Set<String> getEventSet(String projectId, String filterPattern, boolean trimToLast) throws IOException {
    if (Strings.isNullOrEmpty(projectId)) {
      // return null;
    }
    if (Strings.isNullOrEmpty(filterPattern)) {
      return getEventSet(projectId);
    }
    long begin = System.currentTimeMillis();
    String[] levelArray = WebInterfaceCommonUtils.split2LevelArray(filterPattern);
    if (levelArray == null || levelArray.length == 0) {
      throw new IOException("Invalid filter pattern:" + filterPattern);
    }

    Map<Object, Object> condition = new HashMap<Object, Object>();
    for (int i = 0; i < levelArray.length; i++) {
      if (!Strings.isNullOrEmpty(levelArray[i])) {
        condition.put("l" + i, levelArray[i]);
      }
    }
    EventListQueryDescriptor descriptor = new EventListQueryDescriptor(projectId, null, condition);

    CacheContainer cc = getEventSet(descriptor);
    long after = System.currentTimeMillis();
    logCC(cc);
    LOGGER.info("Get CC using: " + (after - begin) + " milliseconds.\n");
    Set<String> set = cc.getEventStringSet();
    if (!trimToLast) {
      return set;
    }
    return WebInterfaceCommonUtils.trim2Last(set, '.', levelArray.length);
  }

  public Set<String> getEventSet(String projectId, String filterPattern) throws IOException {
    return getEventSet(projectId, filterPattern, false);
  }

  public Set<String> getDistinctedEventList(EventListQueryDescriptor descriptor) {
    long begin = System.currentTimeMillis();
    CacheContainer cc = getEventSet(descriptor);
    String targetRow = descriptor.getTargetRow();
    logCC(cc);
    Set<String> distinctRow = cc.getDistinctedSet(targetRow);
    if (distinctRow == null) {
      String distinctString = null;
      Set<Map<String, String>> events = cc.getEventMapSet();
      distinctRow = new TreeSet<String>(new Comparator<String>() {
        public int compare(String o1, String o2) {
          return o1.compareToIgnoreCase(o2);
        }
      });
      for (Map<String, String> m : events) {
        distinctString = m.get(targetRow);
        if (!Strings.isNullOrEmpty(distinctString)) {
          distinctRow.add(distinctString);
        }
      }
      cc.putDistinctedSet(targetRow, distinctRow);
    }
    long after = System.currentTimeMillis();
    LOGGER.info("Get distincted event row using: " + (after - begin) + " milliseconds.\n");
    return distinctRow;
  }

  public CacheContainer getEventSet(EventListQueryDescriptor descriptor) {
    LOGGER.info(descriptor);
    if (descriptor == null || Strings.isNullOrEmpty(descriptor.getProjectId())) {
      return null;
    }

    String projectId = descriptor.getProjectId();
    String targetRow = descriptor.getTargetRow();
    Map<Object, Object> condition = descriptor.getCondition();
    String id = null;
    boolean onlyDistinct = false;
    if (condition == null || condition.isEmpty()) {
      onlyDistinct = true;
      id = projectId + "." + "all";
    } else {
      id = projectId + "." + condition;
    }
    boolean needDistinct = !Strings.isNullOrEmpty(targetRow);
    Cache cache = null;
    cache = cacheManager.getCache(CacheKey.FILTERED_EVENTS.name());

    if (cache == null) {
      cache = new Cache(new CacheConfiguration(CacheKey.FILTERED_EVENTS.name(), WebInterfaceConstants.CACHE_OBJ_NUM_5K)
                          .overflowToDisk(WebInterfaceConstants.CACHE_MEMORY)
                          .eternal(WebInterfaceConstants.CACHE_TRANSIENT)
                          .timeToIdleSeconds(WebInterfaceConstants.CACHE_DURATION_5MIN)
                          .timeToLiveSeconds(WebInterfaceConstants.CACHE_DURATION_10MIN));
      cacheManager.addCache(cache);
    }

    Element element = cache.get(id);

    CacheContainer cc = null;
    if (element != null) {
      cc = (CacheContainer) element.getObjectValue();
      if (onlyDistinct && cc != null && cc.getDistinctedSet(targetRow) == null) {
        Set<String> s = new TreeSet<String>(new Comparator<String>() {
          public int compare(String o1, String o2) {
            return o1.compareToIgnoreCase(o2);
          }
        });
        s.addAll(doOnlyDistinct(descriptor));
        cc.putDistinctedSet(targetRow, s);
        LOGGER.info("Using cache(" + cache.getName() + "), but do distinct only.");
      } else {
        LOGGER.info("Using cache(" + cache.getName() + ")");
      }
      return (CacheContainer) element.getObjectValue();
    }
    LOGGER.info("Query");
    DBCollection coll = db.getCollection("events_list");

    Set<Map<String, String>> eventsMapSet = null;
    Set<String> eventsStringSet = null;
    if (onlyDistinct) {
      cc = new CacheContainer(id, 0);
      cc.putDistinctedSet(targetRow, doOnlyDistinct(descriptor));
    } else {
      DBObject query = new BasicDBObject();
      query.put(TBL_USER_PROPERTIES_PROJECT_ID, projectId);
      if (condition != null && !condition.isEmpty()) {
        query.putAll(condition);
      }
      LOGGER.info("QueryObject:" + query);
      int cnt = (int) coll.count(query);
      cc = new CacheContainer(id, cnt);

      LOGGER.info("Match event count:" + cnt);
      if (cnt == 0) {
        element = new Element(id, cc);
        cache.put(element);
        return cc;
      }

      eventsMapSet = cc.getEventMapSet();
      eventsStringSet = cc.getEventStringSet();

      LOGGER.info("Inited eventsMapSet:\t" + eventsMapSet.size() + " elements");
      LOGGER.info("Inited eventsStringSet:\t" + eventsStringSet.size() + " elements");

      DBCursor cursor = coll.find(query);

      DBObject next = null;
      Map<String, String> singleMap = null;
      Object tmpL0 = null;
      Object tmpL1 = null;
      Object tmpL2 = null;
      Object tmpL3 = null;
      Object tmpL4 = null;
      Object tmpL5 = null;
      StringBuilder sb = null;
      while (cursor.hasNext()) {
        next = cursor.next();
        singleMap = new HashMap<String, String>();
        sb = new StringBuilder();
        tmpL0 = next.get(l0.name());
        tmpL1 = next.get(l1.name());
        tmpL2 = next.get(l2.name());
        tmpL3 = next.get(l3.name());
        tmpL4 = next.get(l4.name());
        tmpL5 = next.get(l5.name());

        put2MapIgnoreEmpty(singleMap, l0.name(), tmpL0);
        put2MapIgnoreEmpty(singleMap, l1.name(), tmpL1);
        put2MapIgnoreEmpty(singleMap, l2.name(), tmpL2);
        put2MapIgnoreEmpty(singleMap, l3.name(), tmpL3);
        put2MapIgnoreEmpty(singleMap, l4.name(), tmpL4);
        put2MapIgnoreEmpty(singleMap, l5.name(), tmpL5);

        appendStringBuilder(sb, tmpL0);
        appendStringBuilder(sb, tmpL1);
        appendStringBuilder(sb, tmpL2);
        appendStringBuilder(sb, tmpL3);
        appendStringBuilder(sb, tmpL4);
        appendStringBuilder(sb, tmpL5);

        eventsMapSet.add(singleMap);
        eventsStringSet.add(sb.toString());
      }
      LOGGER.info("EventMapSet:\t" + eventsMapSet.size() + " elements.");
      LOGGER.info("EventStringSet:\t" + eventsStringSet.size() + " elements.");
      if (needDistinct) {
        LOGGER.info("Distinct elements.");
        Set<String> distinctRow = new TreeSet<String>(new Comparator<String>() {
          public int compare(String o1, String o2) {
            return o1.compareToIgnoreCase(o2);
          }
        });
        String distinctString = null;
        for (Map<String, String> m : eventsMapSet) {
          distinctString = m.get(targetRow);
          if (!Strings.isNullOrEmpty(distinctString)) {
            distinctRow.add(distinctString);
          }
        }
        cc.putDistinctedSet(targetRow, distinctRow);
      }
    }
    element = new Element(id, cc);
    cache.put(element);
    return cc;
  }

  private Set<String> doOnlyDistinct(EventListQueryDescriptor descriptor) {
    String projectId = descriptor.getProjectId();
    String targetRow = descriptor.getTargetRow();
    DBCollection coll = db.getCollection("events_list");
    DBObject query = new BasicDBObject();
    query.put(TBL_USER_PROPERTIES_PROJECT_ID, projectId);
    Set<String> s = new TreeSet<String>(new Comparator<String>() {
      public int compare(String o1, String o2) {
        return o1.compareToIgnoreCase(o2);
      }
    });
    s.addAll(coll.distinct(targetRow, query));
    return s;
  }

  public void logCC(CacheContainer cc) {
    if (cc == null) {
      LOGGER.info("[CC]: null cc");
      return;
    }
    LOGGER.info("[CC-ID]:\t" + cc.getId());
    LOGGER.info("[CC-EventMapSize]:\t" + cc.getEventMapSet().size());
    LOGGER.info("[CC-EventStringSize]:\t" + cc.getEventStringSet().size());
    LOGGER.info("[CC-DistinctedRowMap]:");
    for (String s : cc.getDistinctedEventRowMap().keySet()) {
      LOGGER.info("|=>\t[Row-" + s + "]:\t" + cc.getDistinctedSet(s).size());
    }
  }
}
