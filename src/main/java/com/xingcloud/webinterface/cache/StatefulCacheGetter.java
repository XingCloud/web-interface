package com.xingcloud.webinterface.cache;

import static com.xingcloud.webinterface.conf.WebInterfaceConfig.DS_MOCK;
import static com.xingcloud.webinterface.conf.WebInterfaceConfig.ENABLE_REDIS_CACHE;

import com.xingcloud.maincache.InterruptQueryException;
import com.xingcloud.maincache.XCacheException;
import com.xingcloud.webinterface.exception.ParseIncrementalException;
import com.xingcloud.webinterface.model.StatefulCache;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import org.apache.log4j.Logger;

import java.util.concurrent.Callable;

public class StatefulCacheGetter implements Callable<StatefulCache> {
  private static final Logger LOGGER = Logger.getLogger(StatefulCacheGetter.class);
  private FormulaQueryDescriptor fqd;

  private TimeUseAccumulator timeUseAccumulator;

  public StatefulCacheGetter(FormulaQueryDescriptor fqd, TimeUseAccumulator timeUseAccumulator) {
    super();
    this.fqd = fqd;
    this.timeUseAccumulator = timeUseAccumulator;
  }

  // 在这个方法里处理一切有关异常和monitor的事务
  public StatefulCache call() throws XCacheException, ParseIncrementalException, InterruptQueryException {
    if (DS_MOCK) {
      try {
        return DebugCacheChecker.getInstance().checkCache(fqd);
      } catch (ParseIncrementalException e) {
        e.printStackTrace();
      } catch (InterruptQueryException e) {
        e.printStackTrace();
      }
    }
    StatefulCache statefulCache = null;
    long t1, t2;
    // 从Redis里拿结果
    if (ENABLE_REDIS_CACHE) {
      String basic = "Error occurred in loading redis cache";
      t1 = System.currentTimeMillis();
      try {
        statefulCache = RedisCacheChecker.getInstance().checkCache(fqd);
      } catch (XCacheException e) {
        LOGGER.error(basic + " - " + fqd);
        throw e;
      } catch (ParseIncrementalException e) {
        LOGGER.error(basic + " because cannot parse incremental - " + fqd);
        throw e;
      } catch (InterruptQueryException e) {
        LOGGER.error(basic + " because this query should be interrupt - " + fqd);
        throw e;
      } finally {
        t2 = System.currentTimeMillis();
        this.timeUseAccumulator.updateRedisTimeUse(t2 - t1);
      }
    }
    return statefulCache;
  }

}
