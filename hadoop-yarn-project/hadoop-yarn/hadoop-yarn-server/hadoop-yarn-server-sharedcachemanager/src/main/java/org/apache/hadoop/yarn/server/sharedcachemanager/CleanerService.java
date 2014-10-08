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

package org.apache.hadoop.yarn.server.sharedcachemanager;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.sharedcachemanager.metrics.CleanerMetrics;
import org.apache.hadoop.yarn.server.sharedcachemanager.store.SCMStore;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * The cleaner service that maintains the shared cache area, and cleans up stale
 * entries on a regular basis.
 */
@Private
@Evolving
public class CleanerService extends CompositeService {
  /**
   * Priority of the cleaner shutdown hook.
   */
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;
  /**
   * The name of the global cleaner lock that the cleaner creates to indicate
   * that a cleaning process is in progress.
   */
  public static final String GLOBAL_CLEANER_PID = ".cleaner_pid";

  private static final Log LOG = LogFactory.getLog(CleanerService.class);

  private Configuration conf;
  private AppChecker appChecker;
  private CleanerMetrics metrics;
  private ScheduledExecutorService scheduler;
  private final SCMStore store;
  private final AtomicBoolean cleanerTaskRunning;

  public CleanerService(SCMStore store) {
    super("CleanerService");
    this.store = store;
    this.cleanerTaskRunning = new AtomicBoolean();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    this.conf = conf;

    appChecker = createAppCheckerService(conf);
    addService(appChecker);

    // create a single threaded scheduler service that services the cleaner task
    ThreadFactory tf =
        new ThreadFactoryBuilder().setNameFormat("Shared cache cleaner").build();
    scheduler = Executors.newSingleThreadScheduledExecutor(tf);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    if (!writeGlobalCleanerPidFile()) {
      throw new YarnException("The global cleaner pid file already exists!");
    }

    this.metrics = CleanerMetrics.initSingleton(conf);

    // Start dependent services (i.e. AppChecker)
    super.serviceStart();

    Runnable task =
        CleanerTask.create(conf, appChecker, store, metrics,
            cleanerTaskRunning, true);
    long periodInMinutes = getPeriod(conf);
    scheduler.scheduleAtFixedRate(task, getInitialDelay(conf), periodInMinutes,
        TimeUnit.MINUTES);
    LOG.info("Scheduled the shared cache cleaner task to run every "
        + periodInMinutes + " minutes.");
  }

  @Override
  protected void serviceStop() throws Exception {
    LOG.info("Shutting down the background thread.");
    scheduler.shutdownNow();
    try {
      if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
        LOG.warn("Gave up waiting for the cleaner task to shutdown.");
      }
    } catch (InterruptedException e) {
      LOG.warn("The cleaner service was interrupted while shutting down the task.",
          e);
    }
    LOG.info("The background thread stopped.");

    removeGlobalCleanerPidFile();

    super.serviceStop();
  }

  @VisibleForTesting
  AppChecker createAppCheckerService(Configuration conf) {
    return SharedCacheManager.createAppCheckerService(conf);
  }

  /**
   * If no other cleaner task is running, execute an on-demand cleaner task.
   * 
   * @return true if the cleaner task was started, false if there was already a
   *         cleaner task running.
   */
  protected boolean runCleanerTask() {

    if (!this.cleanerTaskRunning.compareAndSet(false, true)) {
      LOG.warn("A cleaner task is already running. "
          + "A new on-demand cleaner task will not be submitted.");
      return false;
    }

    Runnable task =
        CleanerTask.create(conf, appChecker, store, metrics,
            cleanerTaskRunning, false);
    // this is a non-blocking call (it simply submits the task to the executor
    // queue and returns)
    this.scheduler.execute(task);
    /*
     * We return true if the task is accepted for execution by the executor. Two
     * notes: 1. There is a small race here between a scheduled task and an
     * on-demand task. If the scheduled task happens to start after we check/set
     * cleanerTaskRunning, but before we call execute, we will get two tasks
     * that run back to back. Luckily, since we have already set
     * cleanerTaskRunning, the scheduled task will do nothing and the on-demand
     * task will clean. 2. We do not need to set the cleanerTaskRunning boolean
     * back to false because this will be done in the task itself.
     */
    return true;
  }

  /**
   * To ensure there are not multiple instances of the SCM running on a given
   * cluster, a global pid file is used. This file contains the hostname of the
   * machine that owns the pid file.
   * 
   * @return true if the pid file was written, false otherwise
   * @throws IOException
   */
  private boolean writeGlobalCleanerPidFile() throws YarnException {
    String root =
        conf.get(YarnConfiguration.SHARED_CACHE_ROOT,
            YarnConfiguration.DEFAULT_SHARED_CACHE_ROOT);
    Path pidPath = new Path(root, GLOBAL_CLEANER_PID);
    try {
      FileSystem fs = FileSystem.get(this.conf);

      if (fs.exists(pidPath)) {
        return false;
      }

      FSDataOutputStream os = fs.create(pidPath, false);
      // write the hostname and the process id in the global cleaner pid file
      final String ID = ManagementFactory.getRuntimeMXBean().getName();
      os.writeUTF(ID);
      os.close();
      // add it to the delete-on-exit to ensure it gets deleted when the JVM
      // exits
      fs.deleteOnExit(pidPath);
    } catch (IOException e) {
      throw new YarnException(e);
    }
    LOG.info("Created the global cleaner pid file at " + pidPath.toString());
    return true;
  }

  private void removeGlobalCleanerPidFile() {
    try {
      FileSystem fs = FileSystem.get(this.conf);
      String root =
          conf.get(YarnConfiguration.SHARED_CACHE_ROOT,
              YarnConfiguration.DEFAULT_SHARED_CACHE_ROOT);

      Path pidPath = new Path(root, GLOBAL_CLEANER_PID);


      fs.delete(pidPath, false);
      LOG.info("Removed the global cleaner pid file at " + pidPath.toString());
    } catch (IOException e) {
      LOG.error(
          "Unable to remove the global cleaner pid file! The file may need "
              + "to be removed manually.", e);
    }
  }

  private static int getInitialDelay(Configuration conf) {
    int initialDelayInMinutes =
        conf.getInt(YarnConfiguration.SCM_CLEANER_INITIAL_DELAY,
            YarnConfiguration.DEFAULT_SCM_CLEANER_INITIAL_DELAY);
    // negative value is invalid; use the default
    if (initialDelayInMinutes < 0) {
      throw new HadoopIllegalArgumentException("Negative initial delay value: "
          + initialDelayInMinutes
          + ". The initial delay must be greater than zero.");
    }
    return initialDelayInMinutes;
  }

  private static int getPeriod(Configuration conf) {
    int periodInMinutes =
        conf.getInt(YarnConfiguration.SCM_CLEANER_PERIOD,
            YarnConfiguration.DEFAULT_SCM_CLEANER_PERIOD);
    // non-positive value is invalid; use the default
    if (periodInMinutes <= 0) {
      throw new HadoopIllegalArgumentException("Non-positive period value: "
          + periodInMinutes
          + ". The cleaner period must be greater than or equal to zero.");
    }
    return periodInMinutes;
  }
}
