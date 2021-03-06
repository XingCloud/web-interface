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
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.log4j.Logger;

public class QueueSingleQueryTask extends AbstractQueueQueryTask {
  private static final Logger LOGGER = Logger.getLogger(QueueSingleQueryTask.class);
  private FormulaQueryDescriptor descriptor;

  public QueueSingleQueryTask(FormulaQueryDescriptor descriptor) {
    this.descriptor = descriptor;
  }

  @Override
  public void run() {
    try {
      doQuery();
    } catch (Exception e) {
      LOGGER.error("[QUERY] - Query failed - " + e.getMessage(), e);
    }
  }

  private void doQuery() throws PlanException, JsonProcessingException, XRemoteQueryException {
    if (DS_LP) {
      LogicalPlan logicalPlan;
      Submit submit = (Submit) WebServiceProvider.provideService(WEB_SERVICE_ID);
      if (submit == null) {
        throw new XRemoteQueryException("There is no corresponding web-service");
      }
      ObjectMapper mapper = DEFAULT_DRILL_CONFIG.getMapper();
      logicalPlan = descriptor.toLogicalPlain();
      if (logicalPlan == null) {
        LOGGER.error("[QUERY] - Descriptor has generated null lp string - " + descriptor.getKey());
      } else {
        String planString = mapper.writeValueAsString(logicalPlan);
        writeLPString2LocalLog(planString);
//        LOGGER.info("[LP-STRING]\n" + planString);
        if (submit.submit(descriptor.getKey(), planString, PLAN)) {
          LOGGER.info("[QUERY] - LP added to remote queue - " + descriptor.getKey());
        } else {
          LOGGER.info("[QUERY] - LP rejected by remote queue - " + descriptor.getKey());
        }
      }
    } else {
      LOGGER.info("[QUERY] - LP mock added to remote queue - " + descriptor.getKey());
    }
  }

}
