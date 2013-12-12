package com.xingcloud.webinterface.exec;

import static com.xingcloud.webinterface.conf.WebInterfaceConfig.UI_CHECK;

import com.xingcloud.webinterface.cache.UITableChecker;
import com.xingcloud.webinterface.exception.UICheckException;
import com.xingcloud.webinterface.exception.UICheckTimeoutException;
import com.xingcloud.webinterface.exception.XQueryException;
import com.xingcloud.webinterface.model.formula.FormulaParameterContainer;
import com.xingcloud.webinterface.thread.XCacheGetExecutorServiceProvider;
import org.apache.commons.collections.CollectionUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class AbstractQueryExecutor implements QueryExecutor {

  protected void checkUITableConcurrently(List<FormulaParameterContainer> containers) throws UICheckException,
    XQueryException, UICheckTimeoutException {
    if (!UI_CHECK || CollectionUtils.isEmpty(containers)) {
      return;
    }
    Set<String> distinctProjectIdSet = new HashSet<String>();
    Map<String, Future<Boolean>> futureMap = new HashMap<String, Future<Boolean>>();

    for (FormulaParameterContainer fpc : containers) {
      distinctProjectIdSet.add(fpc.getProjectId());
    }

    ExecutorService service = XCacheGetExecutorServiceProvider.getService();
    Future<Boolean> f;
    for (String projectId : distinctProjectIdSet) {
      f = service.submit(new UITableChecker(projectId));
      futureMap.put(projectId, f);
    }

    Boolean b;
    long t1, t2, elapse, wait = 60000l, timeUse = 0;

    try {
      for (Entry<String, Future<Boolean>> futureEntry : futureMap.entrySet()) {
        t1 = System.currentTimeMillis();
        b = futureEntry.getValue().get(wait - timeUse, TimeUnit.MILLISECONDS);
        if (b == null || b.booleanValue() == false) {
          throw new UICheckException("Project [" + futureEntry.getKey() + "] does not exist in ui-check table.");
        }

        t2 = System.currentTimeMillis();
        elapse = t2 - t1;
        timeUse += elapse;
      }
    } catch (TimeoutException e) {
      throw new UICheckTimeoutException("Cannot check UI-TABLE because it's already timeout.", e);
    } catch (UICheckException e) {
      throw e;
    } catch (Exception e) {
      throw new UICheckException("Cannot check UI-TABLE because some exception occurred - " + e.toString(), e);
    }
  }
}
