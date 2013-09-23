package com.xingcloud.webinterface.cron;

import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.JobKey.jobKey;
import static org.quartz.TriggerBuilder.newTrigger;
import static org.quartz.TriggerKey.triggerKey;

import com.google.common.base.Strings;
import com.xingcloud.webinterface.conf.WebInterfaceConfig;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.TriggerKey;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.utils.Key;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * User: Z J Wu Date: 13-4-23 Time: 下午2:30 Package: com.xingcloud.webinterface.cron
 */
public class XScheduler {
  private static final Logger LOGGER = Logger.getLogger(XScheduler.class);
  private static XScheduler instance;

  public synchronized static XScheduler getInstance() {
    if (instance == null) {
      instance = new XScheduler();
    }
    return instance;
  }

  private XScheduler() {
    SchedulerFactory factory = new StdSchedulerFactory();
    try {
      this.scheduler = factory.getScheduler();
      loadCronScheduler();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private Scheduler scheduler;

  private Map<JobKey, JobDetail> jobDetailMap = new HashMap<JobKey, JobDetail>();
  private Map<TriggerKey, Trigger> triggerMap = new HashMap<TriggerKey, Trigger>();

  private void loadCronScheduler() throws Exception {
    LOGGER.info("[XScheduler] - Load cron schedule");
    Configuration configuration = WebInterfaceConfig.getConfiguration();
    Collection jobs = (Collection) configuration.getList("quartz.jobs.job");
    Collection triggers = (Collection) configuration.getList("quartz.triggers.trigger");
    Collection schedules = (Collection) configuration.getList("quartz.schedules.schedule");
    if (CollectionUtils.isEmpty(jobs)) {
      throw new Exception("No any job.");
    }
    if (CollectionUtils.isEmpty(triggers)) {
      throw new Exception("No any trigger.");
    }
    if (CollectionUtils.isEmpty(schedules)) {
      throw new Exception("No any schedule.");
    }

    Class jobClass;
    JobDetail jobDetail;
    JobKey jobKey;
    Trigger trigger;
    TriggerKey triggerKey;
    String str, name, group, jobClassName, triggerCronExpr, jName, jGroup, tName, tGroup;
    int a, b, c;
    for (Object job : jobs) {
      str = job.toString();
      a = str.indexOf('.');
      b = str.indexOf('@');
      group = str.substring(0, a);
      name = str.substring(a + 1, b);
      jobClassName = str.substring(b + 1);

      jobClass = Class.forName(jobClassName);
      jobDetail = newJob(jobClass).withIdentity(name, group).build();
      jobKey = jobDetail.getKey();
      jobDetailMap.put(jobKey, jobDetail);
      LOGGER.info("[XScheduler] - Job(" + jobKey + ") loaded.");
    }

    for (Object t : triggers) {
      str = t.toString();
      a = str.indexOf('.');
      b = str.indexOf('@');
      group = str.substring(0, a);
      name = str.substring(a + 1, b);
      triggerCronExpr = str.substring(b + 1);
      trigger = newTrigger().withIdentity(name, group).withSchedule(cronSchedule(triggerCronExpr)).build();
      triggerKey = trigger.getKey();
      triggerMap.put(triggerKey, trigger);
      LOGGER.info("[XScheduler] - Trigger(" + triggerKey + ") loaded.");
    }

    for (Object schedule : schedules) {
      str = schedule.toString();
      a = str.indexOf('.');
      b = str.indexOf('@');
      c = str.lastIndexOf('.');

      jGroup = str.substring(0, a);
      jName = str.substring(a + 1, b);
      tGroup = str.substring(b + 1, c);
      tName = str.substring(c+1);

      jobKey = jobKey(jName, jGroup);
      jobDetail = jobDetailMap.get(jobKey);
      if (jobDetail == null) {
        continue;
      }
      triggerKey = triggerKey(tName, tGroup);
      trigger = triggerMap.get(triggerKey);
      if (trigger == null) {
        continue;
      }
      this.scheduler.scheduleJob(jobDetail, trigger);
      LOGGER.info("[XScheduler] - Job scheduled - " + jobKey + " - " + triggerKey);
    }
  }

  public void start() throws SchedulerException {
    LOGGER.info("[XScheduler] - Start scheduler.");
    if (this.scheduler != null) {
      this.scheduler.start();
    }
  }

  public void shutdown(boolean waitForCompletion) throws SchedulerException {
    if (this.scheduler != null) {
      this.scheduler.shutdown(waitForCompletion);
    }
  }

  public void fireJob(String group, String name) throws SchedulerException {
    if (Strings.isNullOrEmpty(name)) {
      throw new SchedulerException("Cannot trigger nameless job.");
    }
    String g;
    if (Strings.isNullOrEmpty(group)) {
      g = Key.DEFAULT_GROUP;
    } else {
      g = group;
    }
    JobKey jobKey = JobKey.jobKey(name, g);
    this.scheduler.triggerJob(jobKey);
  }
}
