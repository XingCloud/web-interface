package com.xingcloud.webinterface.cache;

import com.xingcloud.maincache.XCacheException;
import com.xingcloud.webinterface.debug.RandomCacheMaker;
import com.xingcloud.webinterface.model.StatefulCache;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import com.xingcloud.webinterface.utils.ProbabilityGenerator;
import org.apache.log4j.Logger;

import java.text.ParseException;

public class DebugCacheChecker implements CacheChecker {
  private static final Logger LOGGER = Logger.getLogger(DebugCacheChecker.class);
  public static CacheChecker INSTANCE;

  private DebugCacheChecker() {
  }

  public static synchronized CacheChecker getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new DebugCacheChecker();
    }
    return INSTANCE;
  }

  @Override
  public StatefulCache checkCache(FormulaQueryDescriptor descriptor) throws XCacheException {
    if (descriptor == null) {
      return null;
    }

    boolean returnDirectly = ProbabilityGenerator.doWithProbability(0);

    String key = descriptor.getKey();
    if (returnDirectly) {
      LOGGER.info("[CACHE] - MISS - " + key);
      return null;
    }

    StatefulCache sc;
    try {
      sc = RandomCacheMaker.randomCache(descriptor);
    } catch (ParseException e) {
      throw new XCacheException(e);
    }
    LOGGER.info("[CACHE] - HIT, DEBUG - " + key);
    return sc;
  }
}
