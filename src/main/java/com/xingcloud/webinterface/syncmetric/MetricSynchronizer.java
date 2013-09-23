package com.xingcloud.webinterface.syncmetric;

import com.xingcloud.webinterface.enums.SyncType;
import com.xingcloud.webinterface.exception.MetricSyncException;
import com.xingcloud.webinterface.mongo.MongoDBOperation;
import com.xingcloud.webinterface.syncmetric.model.AbstractSync;
import org.apache.log4j.Logger;

import java.util.concurrent.Callable;

public class MetricSynchronizer implements Callable<Boolean> {
  private static final Logger LOGGER = Logger.getLogger(MetricSynchronizer.class);
  private AbstractSync as;

  private SyncType type;

  private MongoDBOperation mongoDBOperation;

  public MetricSynchronizer(AbstractSync as, SyncType type) {
    this.mongoDBOperation = MongoDBOperation.getInstance();
    this.as = as;
    this.type = type;
  }

  public boolean sync() throws MetricSyncException {
    if (!as.isValid()) {
      LOGGER.error("Sync object is not valid - " + as);
      return false;
    }
    as.trim();
    try {
      switch (type) {
        case SAVE_OR_UPDATE:
          mongoDBOperation.saveMetric(as);
          break;
        case REMOVE:
          mongoDBOperation.removeMetric(as);
          break;
        default:
          break;
      }
      return true;
    } catch (Exception e) {
      throw new MetricSyncException(e.getMessage(), e);
    }
  }

  public Boolean call() throws Exception {
    if (as == null) {
      return false;
    }
    return sync();
  }

}
