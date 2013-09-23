package com.xingcloud.webinterface.cache;

import static com.xingcloud.basic.mail.XMail.sendNewExceptionMail;
import static com.xingcloud.maincache.redis.RedisUIChecker.UI_CHECK_PREFIX;
import static com.xingcloud.webinterface.conf.WebInterfaceConfig.UI_CHECK;

import com.google.common.base.Strings;
import com.xingcloud.maincache.XCacheException;
import com.xingcloud.maincache.redis.RedisUIChecker;
import com.xingcloud.webinterface.exception.UICheckException;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

public class UITableChecker implements Callable<Boolean> {
  private static final Logger LOGGER = Logger.getLogger(UITableChecker.class);

  public static final Set<String> LOCAL_UI_CACHE = Collections.synchronizedSet(new HashSet<String>(600));

  public static void loadInitedUITables() {
    if (UI_CHECK) {
      try {
        LOCAL_UI_CACHE.addAll(RedisUIChecker.getInstance().loadAll());
        LOGGER.info("[UI-CHECK] - Inited exists ui tables - " + LOCAL_UI_CACHE);
      } catch (XCacheException e) {
        e.printStackTrace();
      }
    }
  }

  private String projectId;

  public UITableChecker(String projectId) {
    super();
    this.projectId = projectId;
  }

  public Boolean call() throws Exception {
    if (Strings.isNullOrEmpty(projectId)) {
      throw new UICheckException("Must appoint a specific project id to check.");
    }
    String key = UI_CHECK_PREFIX + projectId;

    boolean exists = LOCAL_UI_CACHE.contains(key);
    if (exists) {
      LOGGER.info("[UI-CHECK] - LOCAL - " + key);
    } else {
      try {
        exists = RedisUIChecker.getInstance().check(projectId);
      } catch (Exception e) {
        sendNewExceptionMail(e);
        throw e;
      }
      if (exists) {
        LOCAL_UI_CACHE.add(key);
      }
      LOGGER.info("[UI-CHECK] - REDIS - " + key);
    }

    return exists;
  }

  public static void getLocalCache() {
  }

  public static void main(String[] args) {
  }

}
