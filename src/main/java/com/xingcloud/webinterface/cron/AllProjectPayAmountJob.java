package com.xingcloud.webinterface.cron;

import static com.xingcloud.basic.utils.DateUtils.date2Short;
import static com.xingcloud.basic.utils.DateUtils.yesterday;
import static com.xingcloud.webinterface.conf.WebInterfaceConfig.DEBUG;
import static com.xingcloud.webinterface.enums.CacheState.ACCURATE;
import static com.xingcloud.webinterface.enums.CommonQueryType.NORMAL;
import static com.xingcloud.webinterface.enums.DateTruncateType.PASS;
import static com.xingcloud.webinterface.enums.Function.SUM;
import static com.xingcloud.webinterface.enums.Interval.DAY;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.CACHE_PREFIX_UIC;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.GENERIC_SEPARATOR;
import static com.xingcloud.webinterface.utils.WebInterfaceConstants.TOTAL_USER;

import com.google.common.base.Strings;
import com.xingcloud.webinterface.cache.StatefulCacheGetter;
import com.xingcloud.webinterface.cache.TimeUseAccumulator;
import com.xingcloud.webinterface.cache.UITableChecker;
import com.xingcloud.webinterface.exec.QueueBatchQueryTask;
import com.xingcloud.webinterface.model.Filter;
import com.xingcloud.webinterface.model.ResultTuple;
import com.xingcloud.webinterface.model.StatefulCache;
import com.xingcloud.webinterface.model.formula.CommonFormulaQueryDescriptor;
import com.xingcloud.webinterface.model.formula.FormulaQueryDescriptor;
import com.xingcloud.webinterface.thread.XCacheGetExecutorServiceProvider;
import com.xingcloud.webinterface.thread.XQueryExecutorServiceProvider;
import org.apache.log4j.Logger;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * User: Z J Wu Date: 13-4-23 Time: 下午2:27 Package: com.xingcloud.webinterface.cron
 */
public class AllProjectPayAmountJob implements Job {

  public static final String PAY_AMOUNT_DATA_OUTPUT = DEBUG ? "e:/pay.data." : "/data/catalina/8081/logs/pay.data.";

  private static final Logger LOGGER = Logger.getLogger(AllProjectPayAmountJob.class);

  // 缓存命中
  private final int RUNNING = 0;
  // 缓存命中
  private final int DONE = 1;
  // 异常
  private final int ERROR = 2;
  // 重试n次后仍无法获得数据, 超时
  private final int TIMEOUT = 3;

  private class PayAmountJob implements Comparable<PayAmountJob> {

    private String projectId;

    private FormulaQueryDescriptor descriptor;

    private int status;

    private ResultTuple value;

    private int counter;

    private Future<StatefulCache> future;

    private PayAmountJob(String projectId, FormulaQueryDescriptor descriptor) {
      this.projectId = projectId;
      this.descriptor = descriptor;
      this.status = RUNNING;
    }

    public boolean isFinished() {
      return this.status >= 1;
    }

    public void finishSuccessfully() {
      this.status = DONE;
    }

    public void finishWithError() {
      this.status = ERROR;
    }

    public void finishWithTimeout() {
      this.status = TIMEOUT;
    }

    public void executeOnce() {
      ++this.counter;
    }

    private Future<StatefulCache> getFuture() {
      return future;
    }

    private void setFuture(Future<StatefulCache> future) {
      this.future = future;
    }

    private String getProjectId() {
      return projectId;
    }

    private void setProjectId(String projectId) {
      this.projectId = projectId;
    }

    private FormulaQueryDescriptor getDescriptor() {
      return descriptor;
    }

    private void setDescriptor(FormulaQueryDescriptor descriptor) {
      this.descriptor = descriptor;
    }

    private int getStatus() {
      return status;
    }

    private void setStatus(int status) {
      this.status = status;
    }

    private ResultTuple getValue() {
      return value;
    }

    private void setValue(ResultTuple value) {
      this.value = value;
    }

    private int getCounter() {
      return counter;
    }

    private void setCounter(int counter) {
      this.counter = counter;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      PayAmountJob that = (PayAmountJob) o;

      if (projectId != null ? !projectId.equals(that.projectId) : that.projectId != null) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return projectId != null ? projectId.hashCode() : 0;
    }

    @Override public int compareTo(PayAmountJob o) {
      if (o == null) {
        return 1;
      }
      int thisStatus = getStatus();
      int thatStatus = o.getStatus();
      if (thisStatus == DONE && thatStatus == DONE) {
        ResultTuple thisRT = getValue();
        ResultTuple thatRT = o.getValue();
        Long thisPayAmount = (thisRT == null ? 0 : thisRT.getEstimateSum().longValue()
        );
        Long thatPayAmount = (thatRT == null ? 0 : thatRT.getEstimateSum().longValue()
        );
        return -thisPayAmount.compareTo(thatPayAmount);
      } else if (thisStatus == DONE && thatStatus != DONE) {
        return -1;
      } else if (thisStatus != DONE && thatStatus == DONE) {
        return 1;
      } else {
        return getProjectId().compareTo(o.getProjectId());
      }
    }

    @Override public String toString() {
      String status;
      if (this.status == RUNNING) {
        status = "RUN";
      } else if (this.status == DONE) {
        status = "DON";
      } else if (this.status == ERROR) {
        status = "ERR";
      } else {
        status = "TIM";
      }
      return "PAJ|" + status + "|(" + projectId + "):" + value + "@" + descriptor
        .getRealBeginDate() + " in " + counter + " times";
    }
  }

  private void query(List<PayAmountJob> jobs) {
    ExecutorService service = XQueryExecutorServiceProvider.getService();
    Collection<FormulaQueryDescriptor> coll;
    for (PayAmountJob job : jobs) {
      if (job.isFinished()) {
        continue;
      }
      coll = new HashSet<FormulaQueryDescriptor>(1);
      coll.add(job.getDescriptor());
      job.executeOnce();
      service.execute(new QueueBatchQueryTask(coll));
    }
  }

  private boolean checkCache(List<PayAmountJob> jobs) {
    TimeUseAccumulator accumulator = new TimeUseAccumulator();
    ExecutorService service = XCacheGetExecutorServiceProvider.getService();
    Future<StatefulCache> cacheFuture;
    FormulaQueryDescriptor descriptor;
    for (PayAmountJob job : jobs) {
      if (job.isFinished()) {
        continue;
      }
      descriptor = job.getDescriptor();
      cacheFuture = service.submit(new StatefulCacheGetter(descriptor, accumulator));
      job.setFuture(cacheFuture);
    }

    StatefulCache sc;
    Map<Object, ResultTuple> cachedResultTupleMap;
    int total = jobs.size();
    int allReadyFinished = 0;
    int thisTimeFinished = 0;
    for (PayAmountJob job : jobs) {
      if (job.isFinished()) {
        ++allReadyFinished;
        continue;
      }
      cacheFuture = job.getFuture();
      try {
        sc = cacheFuture.get();
      } catch (Exception e) {
        LOGGER.error("[PAY-AMOUNT-JOB] - Cannot get cache - " + job.getProjectId(), e);
        job.finishWithError();
        continue;
      }
      if (sc == null) {
        continue;
      }
      cachedResultTupleMap = sc.getContent();
      if (cachedResultTupleMap == null) {
        continue;
      }

      if (ACCURATE.equals(sc.getState())) {
        job.finishSuccessfully();
        ++thisTimeFinished;

        for (Entry<Object, ResultTuple> e : cachedResultTupleMap.entrySet()) {
          job.setValue(e.getValue());
          break;
        }
      }
    }
    return (thisTimeFinished + allReadyFinished) == total;
  }

  @Override
  public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
    LOGGER.info("[PAY-AMOUNT-JOB] - Start executing.");

    Set<String> uiTables;
    if (DEBUG) {
      uiTables = new HashSet<String>(1);
      uiTables.add(CACHE_PREFIX_UIC + "tencent-16488");
      uiTables.add(CACHE_PREFIX_UIC + "tencent-18894");
      uiTables.add(CACHE_PREFIX_UIC + "tencent-19089");
      uiTables.add(CACHE_PREFIX_UIC + "tencent-28433");
      uiTables.add(CACHE_PREFIX_UIC + "22find");
      uiTables.add(CACHE_PREFIX_UIC + "opiece");
      uiTables.add(CACHE_PREFIX_UIC + "age");
      uiTables.add(CACHE_PREFIX_UIC + "v9-v9");
      uiTables.add(CACHE_PREFIX_UIC + "sof-dsk");
      uiTables.add(CACHE_PREFIX_UIC + "xlfc");
    } else {
      uiTables = UITableChecker.LOCAL_UI_CACHE;
    }
    int size = uiTables.size();
    String projectId;
    String operationDate = date2Short(yesterday());

    List<PayAmountJob> jobs = new ArrayList<PayAmountJob>(size);
    FormulaQueryDescriptor descriptor;
    PayAmountJob payAmountJob;
    for (String uiTable : uiTables) {
      projectId = uiTable.substring(CACHE_PREFIX_UIC.length());
      if (Strings.isNullOrEmpty(projectId)) {
        continue;
      }
      descriptor = new CommonFormulaQueryDescriptor(projectId, operationDate, operationDate, "pay.gross.*", TOTAL_USER,
                                                    Filter.ALL, 1.0d, DAY, NORMAL);
      descriptor.addFunction(SUM);
      descriptor.setDateTruncateType(PASS);
      payAmountJob = new PayAmountJob(projectId, descriptor);
      jobs.add(payAmountJob);
      LOGGER.info("[QUARTZ-JOB] - Job - " + payAmountJob);
    }

    // 查询最多执行n轮, 检测最多n+1轮
    int retryTimes = 20;
    boolean allFinished = false;
    for (int i = 0; i < retryTimes; i++) {
      LOGGER.info("[QUARTZ-JOB] - Load jobs' cache, round " + (i + 1));
      allFinished = checkCache(jobs);
      if (allFinished) {
        LOGGER.info("[QUARTZ-JOB] - All jobs' cache hit.");
        break;
      }
      LOGGER.info("[QUARTZ-JOB] - Not all jobs' cache hit, submit query job.");
      query(jobs);
      try {
        if (DEBUG) {
          TimeUnit.SECONDS.sleep(1l);
        } else {
          TimeUnit.MINUTES.sleep(3l);
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    if (!allFinished || !checkCache(jobs)) {
      LOGGER.info("[QUARTZ-JOB] - Not all jobs' cache hit, final checking finished.");
    }

    for (PayAmountJob job : jobs) {
      if (!job.isFinished()) {
        job.finishWithTimeout();
      }
    }
    Collections.sort(jobs);
    File f = new File(PAY_AMOUNT_DATA_OUTPUT + operationDate);
    PrintWriter pw = null;
    ResultTuple rt;
    String valString;
    try {
      pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(f)));
      for (PayAmountJob job : jobs) {
        pw.write(job.getProjectId());
        pw.write(GENERIC_SEPARATOR);
        rt = job.getValue();
        valString = (rt == null ? "-1" : String.valueOf(job.getValue().getEstimateSum().longValue())
        );
        pw.write(valString);
        pw.write(GENERIC_SEPARATOR);
        pw.write(String.valueOf(job.getCounter()));
        pw.write('\n');
        LOGGER.info("[QUARTZ-JOB] - Job summary - " + job);
      }
    } catch (Exception e) {
      LOGGER.error("[QUARTZ-JOB] - Cannot write result data to file - " + f.getAbsolutePath());
    } finally {
      if (pw != null) {
        pw.close();
      }
    }
  }

}
