/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.sharedcachemanager.metrics;

import static org.apache.hadoop.metrics2.impl.MsInfo.SessionId;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsAnnotations;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MetricsSourceBuilder;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;

/**
 * This class is for maintaining the various Cleaner activity statistics and
 * publishing them through the metrics interfaces.
 */
@Private
@Evolving
@Metrics(name = "CleanerActivity", about = "Cleaner service metrics", context = "yarn")
public class CleanerMetrics {
  public static final Log LOG = LogFactory.getLog(CleanerMetrics.class);
  private final MetricsRegistry registry = new MetricsRegistry("cleaner");

  enum Singleton {
    INSTANCE;

    CleanerMetrics impl;

    synchronized CleanerMetrics init(Configuration conf) {
      if (impl == null) {
        impl = create(conf);
      }
      return impl;
    }
  }

  public static CleanerMetrics initSingleton(Configuration conf) {
    return Singleton.INSTANCE.init(conf);
  }

  public static CleanerMetrics getInstance() {
    CleanerMetrics topMetrics = Singleton.INSTANCE.impl;
    if (topMetrics == null)
      throw new IllegalStateException(
          "The CleanerMetics singlton instance is not initialized."
              + " Have you called init first?");
    return topMetrics;
  }

  @Metric("number of deleted files over all runs")
  private MutableCounterLong totalDeletedFiles;

  public long getTotalDeletedFiles() {
    return totalDeletedFiles.value();
  }

  private @Metric("number of deleted files in the last run")
  MutableGaugeLong deletedFiles;

  public long getDeletedFiles() {
    return deletedFiles.value();
  }

  private @Metric("number of processed files over all runs")
  MutableCounterLong totalProcessedFiles;

  public long getTotalProcessedFiles() {
    return totalProcessedFiles.value();
  }

  private @Metric("number of processed files in the last run")
  MutableGaugeLong processedFiles;

  public long getProcessedFiles() {
    return processedFiles.value();
  }

  @Metric("number of file errors over all runs")
  private MutableCounterLong totalFileErrors;

  public long getTotalFileErrors() {
    return totalFileErrors.value();
  }

  private @Metric("number of file errors in the last run")
  MutableGaugeLong fileErrors;

  public long getFileErrors() {
    return fileErrors.value();
  }

  private @Metric("Rate of deleting the files over all runs")
  MutableGaugeInt totalDeleteRate;

  public int getTotalDeleteRate() {
    return totalDeleteRate.value();
  }

  private @Metric("Rate of deleting the files over the last run")
  MutableGaugeInt deleteRate;

  public int getDeleteRate() {
    return deleteRate.value();
  }

  private @Metric("Rate of prcessing the files over all runs")
  MutableGaugeInt totalProcessRate;

  public int getTotalProcessRate() {
    return totalProcessRate.value();
  }

  private @Metric("Rate of prcessing the files over the last run")
  MutableGaugeInt processRate;

  public int getProcessRate() {
    return processRate.value();
  }

  private @Metric("Rate of file errors over all runs")
  MutableGaugeInt totalErrorRate;

  public int getTotalErrorRate() {
    return totalErrorRate.value();
  }

  private @Metric("Rate of file errors over the last run")
  MutableGaugeInt errorRate;

  public int getErrorRate() {
    return errorRate.value();
  }

  private CleanerMetrics() {
  }

  /**
   * The metric source obtained after parsing the annotations
   */
  MetricsSource metricSource;

  /**
   * The start of the last run of cleaner is ms
   */
  private AtomicLong lastRunStart = new AtomicLong(System.currentTimeMillis());

  /**
   * The end of the last run of cleaner is ms
   */
  private AtomicLong lastRunEnd = new AtomicLong(System.currentTimeMillis());

  /**
   * The sum of the durations of the last runs
   */
  private AtomicLong sumOfPastPeriods = new AtomicLong(0);

  public MetricsSource getMetricSource() {
    return metricSource;
  }

  static CleanerMetrics create(Configuration conf) {
    MetricsSystem ms = DefaultMetricsSystem.instance();

    CleanerMetrics metricObject = new CleanerMetrics();
    MetricsSourceBuilder sb = MetricsAnnotations.newSourceBuilder(metricObject);
    final MetricsSource s = sb.build();
    ms.register("cleaner", "The cleaner service of truly shared cache", s);
    metricObject.metricSource = s;
    return metricObject;
  }

  /**
   * Report a delete operation at the current system time
   */
  public void reportAFileDelete() {
    long time = System.currentTimeMillis();
    reportAFileDelete(time);
  }

  /**
   * Report a delete operation at the specified time.
   * Delete implies process as well.
   * @param time
   */
  public void reportAFileDelete(long time) {
    totalProcessedFiles.incr();
    processedFiles.incr();
    totalDeletedFiles.incr();
    deletedFiles.incr();
    updateRates(time);
    lastRunEnd.set(time);
  }

  /**
   * Report a process operation at the current system time
   */
  public void reportAFileProcess() {
    long time = System.currentTimeMillis();
    reportAFileProcess(time);
  }

  /**
   * Report a process operation at the specified time
   * 
   * @param time
   */
  public void reportAFileProcess(long time) {
    totalProcessedFiles.incr();
    processedFiles.incr();
    updateRates(time);
    lastRunEnd.set(time);
  }

  /**
   * Report a process operation error at the current system time
   */
  public void reportAFileError() {
    long time = System.currentTimeMillis();
    reportAFileError(time);
  }

  /**
   * Report a process operation error at the specified time
   * 
   * @param time
   */
  public void reportAFileError(long time) {
    totalProcessedFiles.incr();
    processedFiles.incr();
    totalFileErrors.incr();
    fileErrors.incr();
    updateRates(time);
    lastRunEnd.set(time);
  }

  private void updateRates(long time) {
    long startTime = lastRunStart.get();
    long lastPeriod = time - startTime;
    long sumPeriods = sumOfPastPeriods.get() + lastPeriod;
    float lastRunProcessRate = ((float) processedFiles.value()) / lastPeriod;
    processRate.set(ratePerMsToHour(lastRunProcessRate));
    float totalProcessRateMs = ((float) totalProcessedFiles.value()) / sumPeriods;
    totalProcessRate.set(ratePerMsToHour(totalProcessRateMs));
    float lastRunDeleteRate = ((float) deletedFiles.value()) / lastPeriod;
    deleteRate.set(ratePerMsToHour(lastRunDeleteRate));
    float totalDeleteRateMs = ((float) totalDeletedFiles.value()) / sumPeriods;
    totalDeleteRate.set(ratePerMsToHour(totalDeleteRateMs));
    float lastRunErrorRateMs = ((float) fileErrors.value()) / lastPeriod;
    errorRate.set(ratePerMsToHour(lastRunErrorRateMs));
    float totalErrorRateMs = ((float) totalFileErrors.value()) / sumPeriods;
    totalErrorRate.set(ratePerMsToHour(totalErrorRateMs));
  }

  /**
   * Report the start a new run of the cleaner at the current system time
   */
  public void reportCleaningStart() {
    long time = System.currentTimeMillis();
    reportCleaningStart(time);
  }

  /**
   * Report the start a new run of the cleaner at the specified time
   * 
   * @param time
   */
  public void reportCleaningStart(long time) {
    long lastPeriod = lastRunEnd.get() - lastRunStart.get();
    if (lastPeriod < 0) {
      LOG.error("No operation since last start!");
      lastPeriod = 0;
    }
    lastRunStart.set(time);
    processedFiles.set(0);
    deletedFiles.set(0);
    processRate.set(0);
    deleteRate.set(0);
    errorRate.set(0);
    sumOfPastPeriods.addAndGet(lastPeriod);
    registry.tag(SessionId, Long.toString(time), true);
  }

  static int ratePerMsToHour(float ratePerMs) {
    float ratePerHour = ratePerMs * 1000 * 3600;
    return (int) ratePerHour;
  }

  public static boolean isCleanerMetricRecord(String name) {
    return (name.startsWith("cleaner"));
  }
}
