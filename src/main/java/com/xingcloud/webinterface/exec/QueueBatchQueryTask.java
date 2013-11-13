package com.xingcloud.webinterface.exec;

import static com.xingcloud.qm.service.Submit.SubmitQueryType.PLAN;
import static com.xingcloud.webinterface.conf.WebInterfaceConfig.DS_LP;
import static com.xingcloud.webinterface.plan.Plans.DEFAULT_DRILL_CONFIG;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xingcloud.qm.exceptions.XRemoteQueryException;
import com.xingcloud.qm.service.Submit;
import com.xingcloud.webinterface.exception.PlanException;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import com.xingcloud.webinterface.remote.WebServiceProvider;
import org.apache.commons.collections.CollectionUtils;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class QueueBatchQueryTask extends AbstractQueueQueryTask {
  private static final Logger LOGGER = Logger.getLogger(QueueBatchQueryTask.class);
  private Collection<FormulaQueryDescriptor> descriptors;

  public QueueBatchQueryTask(Collection<FormulaQueryDescriptor> descriptors) {
    this.descriptors = descriptors;
  }

  @Override
  public void run() {
    try {
      doQuery();
    } catch (Exception e) {
      LOGGER.error("[QUERY] - Query failed - " + e.getMessage(), e);
    }
  }

  private void doQuery() throws JsonProcessingException, XRemoteQueryException, PlanException {
    if (CollectionUtils.isEmpty(descriptors)) {
      return;
    }
    LogicalPlan logicalPlan;
    Submit submit = (Submit) WebServiceProvider.provideService(WEB_SERVICE_ID);
    if (submit == null) {
      throw new XRemoteQueryException("There is no corresponding web-service");
    }
    ObjectMapper mapper = DEFAULT_DRILL_CONFIG.getMapper();

    Map<String, String> batch = new HashMap<String, String>(descriptors.size());
    for (FormulaQueryDescriptor descriptor : descriptors) {
      logicalPlan = descriptor.toLogicalPlain();
      if (logicalPlan == null) {
        throw new PlanException("Null logical plan");
      }
      String planString = mapper.writeValueAsString(logicalPlan);
      LOGGER.info("[LP-STRING]\n" + planString);
      if (DS_LP) {
        batch.put(descriptor.getKey(), planString);
//        writeLPString2LocalLog(planString);
      } else {
        LOGGER.info("[QUERY] - LP mock added to remote queue - " + descriptor.getKey());
      }
    }
    if (batch.size() > 0) {
      submit.submitBatch(batch, PLAN);
    }
  }

}
