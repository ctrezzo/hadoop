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
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.sharedcache.SharedCacheUtil;
import org.apache.hadoop.yarn.server.sharedcachemanager.metrics.CleanerMetrics;
import org.apache.hadoop.yarn.server.sharedcachemanager.store.SCMStore;
import org.apache.hadoop.yarn.server.sharedcachemanager.store.SharedCacheResourceReference;

/**
 * The task that runs and cleans up the shared cache area for stale entries and
 * orphaned files. It is expected that only one cleaner task runs at any given
 * point in time.
 */
@Private
@Evolving
class CleanerTask implements Runnable {
  public static final String RENAMED_SUFFIX = "-renamed";
  private static final Log LOG = LogFactory.getLog(CleanerTask.class);

  private final String location;
  private final long sleepTime;
  private final int nestedLevel;
  private final Path root;
  private final FileSystem fs;
  private final AppChecker appChecker;
  private final SCMStore store;
  private final CleanerMetrics metrics;
  private final AtomicBoolean cleanerTaskIsRunning;
  private final boolean isScheduledTask;

  /**
   * Creates a cleaner task based on the configuration. This is provided for
   * convenience.
   * 
   * @param conf
   * @param appChecker
   * @param store
   * @param metrics
   * @param cleanerTaskRunning true if there is another cleaner task currently
   *          running
   * @param isScheduledTask true if the task is a scheduled task
   * @return an instance of a CleanerTask
   */
  public static CleanerTask create(Configuration conf, AppChecker appChecker,
      SCMStore store, CleanerMetrics metrics, AtomicBoolean cleanerTaskRunning,
      boolean isScheduledTask) {
    try {
      // get the root directory for the shared cache
      String location =
          conf.get(YarnConfiguration.SHARED_CACHE_ROOT,
              YarnConfiguration.DEFAULT_SHARED_CACHE_ROOT);

      long sleepTime =
          conf.getLong(YarnConfiguration.SCM_CLEANER_RESOURCE_SLEEP,
              YarnConfiguration.DEFAULT_SCM_CLEANER_RESOURCE_SLEEP);
      int nestedLevel = SharedCacheUtil.getCacheDepth(conf);
      FileSystem fs = FileSystem.get(conf);

      return new CleanerTask(location, sleepTime, nestedLevel, fs, appChecker,
          store, metrics, cleanerTaskRunning, isScheduledTask);
    } catch (IOException e) {
      LOG.error("Unable to obtain the filesystem for the cleaner service", e);
      throw new ExceptionInInitializerError(e);
    }
  }

  /**
   * Creates a cleaner task based on the root directory location and the
   * filesystem.
   */
  CleanerTask(String location, long sleepTime, int nestedLevel, FileSystem fs,
      AppChecker appChecker, SCMStore store, CleanerMetrics metrics,
      AtomicBoolean cleanerTaskIsRunning, boolean isScheduledTask) {
    this.location = location;
    this.sleepTime = sleepTime;
    this.nestedLevel = nestedLevel;
    this.root = new Path(location);
    this.fs = fs;
    this.store = store;
    this.appChecker = appChecker;
    this.metrics = metrics;
    this.cleanerTaskIsRunning = cleanerTaskIsRunning;
    this.isScheduledTask = isScheduledTask;
  }

  public void run() {
    // check if it is a scheduled task
    if (isScheduledTask
        && !this.cleanerTaskIsRunning.compareAndSet(false, true)) {
      // this is a scheduled task and there is already another task running
      LOG.warn("A cleaner task is already running. "
          + "This scheduled cleaner task will do nothing.");
      return;
    }

    try {
      if (!fs.exists(root)) {
        LOG.error("The shared cache root " + location + " was not found. "
            + "The cleaner task will do nothing.");
        return;
      }

      // we're now ready to process the shared cache area
      process();
    } catch (IOException e) {
      LOG.error("Unexpected exception while initializing the cleaner task. "
          + "This task will do nothing,", e);
    } finally {
      // this is set to false regardless of if it is a scheduled or on-demand
      // task
      this.cleanerTaskIsRunning.set(false);
    }
  }

  /**
   * Sweeps and processes the shared cache area to clean up stale and orphaned
   * files.
   */
  void process() {
    // mark the beginning of the run in the metrics
    metrics.reportCleaningStart();
    try {
      // now traverse individual directories and process them
      // the directory structure is specified by the nested level parameter
      // (e.g. 9/c/d/<checksum>)
      StringBuilder pattern = new StringBuilder();
      for (int i = 0; i < nestedLevel; i++) {
        pattern.append("*/");
      }
      pattern.append("*");
      FileStatus[] resources =
          fs.globStatus(new Path(root, pattern.toString()));
      int numResources = resources == null ? 0 : resources.length;
      LOG.info("Processing " + numResources + " resources in the shared cache");
      long beginNano = System.nanoTime();
      if (resources != null) {
        for (FileStatus resource : resources) {
          // check for interruption so it can abort in a timely manner in case
          // of shutdown
          if (Thread.currentThread().isInterrupted()) {
            LOG.warn("The cleaner task was interrupted. Aborting.");
            break;
          }

          if (resource.isDirectory()) {
            processSingleResource(resource);
          } else {
            LOG.warn("Invalid file at path " + resource.getPath().toString()
                +
                " when a directory was expected");
          }
          // add sleep time between cleaning each directory if it is non-zero
          if (sleepTime > 0) {
            Thread.sleep(sleepTime);
          }
        }
      }
      long endNano = System.nanoTime();
      long durationMs = TimeUnit.NANOSECONDS.toMillis(endNano - beginNano);
      LOG.info("Processed " + numResources + " resource(s) in " + durationMs
          +
          " ms.");
    } catch (IOException e1) {
      LOG.error("Unable to complete the cleaner task", e1);
    } catch (InterruptedException e2) {
      Thread.currentThread().interrupt(); // restore the interrupt
    }
  }

  /**
   * Returns a path for the root directory for the shared cache.
   */
  Path getRootPath() {
    return root;
  }

  /**
   * Processes a single shared cache resource directory.
   */
  void processSingleResource(FileStatus resource) {
    Path path = resource.getPath();
    // indicates the processing status of the resource
    ResourceStatus resourceStatus = ResourceStatus.INIT;

    // first, if the path ends with the renamed suffix, it indicates the
    // directory was moved (as stale) but somehow not deleted (probably due to
    // SCM failure); delete the directory
    if (path.toString().endsWith(RENAMED_SUFFIX)) {
      LOG.info("Found a renamed directory that was left undeleted at " +
          path.toString() + ". Deleting.");
      try {
        if (fs.delete(path, true)) {
          resourceStatus = ResourceStatus.DELETED;
        }
      } catch (IOException e) {
        LOG.error("Error while processing a shared cache resource: " + path, e);
      }
    } else {
      // this is the path to the cache resource directory
      // the directory name is the resource key (i.e. a unique identifier)
      String key = path.getName();

      try {
        cleanResourceReferences(key);
      } catch (YarnException e) {
        LOG.error("Exception thrown while removing dead appIds.", e);
      }

      if (store.isResourceEvictable(key, resource)) {
        try {
          /*
           * TODO: There is a race condition between store.removeResource(key)
           * and removeResourceFromCacheFileSystem(path) operations because they
           * do not happen atomically and resources can be uploaded with
           * different file names by the node managers.
           */
          // remove the resource from scm (checks for appIds as well)
          if (store.removeResource(key)) {
            // remove the resource from the file system
            boolean deleted = removeResourceFromCacheFileSystem(path);
            if (deleted) {
              resourceStatus = ResourceStatus.DELETED;
            } else {
              LOG.error("Failed to remove path from the file system."
                  + " Skipping this resource: " + path);
              resourceStatus = ResourceStatus.ERROR;
            }
          } else {
            // we did not delete the resource because it contained application
            // ids
            resourceStatus = ResourceStatus.PROCESSED;
          }
        } catch (IOException e) {
          LOG.error(
              "Failed to remove path from the file system. Skipping this resource: "
                  + path, e);
          resourceStatus = ResourceStatus.ERROR;
        }
      } else {
        resourceStatus = ResourceStatus.PROCESSED;
      }
    }

    // record the processing
    switch (resourceStatus) {
    case DELETED:
      metrics.reportAFileDelete();
      break;
    case PROCESSED:
      metrics.reportAFileProcess();
      break;
    case ERROR:
      metrics.reportAFileError();
      break;
    default:
      LOG.error("Cleaner encountered an invalid status (" + resourceStatus
          + ") while processing resource: " + path.getName());
    }
  }

  private boolean removeResourceFromCacheFileSystem(Path path)
      throws IOException {
    // rename the directory to make the delete atomic
    Path renamedPath = new Path(path.toString() + RENAMED_SUFFIX);
    if (fs.rename(path, renamedPath)) {
      // the directory can be removed safely now
      // log the original path
      LOG.info("Deleting " + path.toString());
      return fs.delete(renamedPath, true);
    } else {
      // we were unable to remove it for some reason: it's best to leave
      // it at that
      LOG.error("We were not able to rename the directory to "
          + renamedPath.toString() + ". We will leave it intact.");
    }
    return false;
  }

  /**
   * Clean all resource references to a cache resource that contain application
   * ids pointing to finished applications. If the resource key does not exist,
   * do nothing.
   * 
   * @param key a unique identifier for a resource
   * @throws YarnException
   */
  private void cleanResourceReferences(String key) throws YarnException {
    Collection<SharedCacheResourceReference> refs =
        store.getResourceReferences(key);
    if (!refs.isEmpty()) {
      Set<SharedCacheResourceReference> refsToRemove =
          new HashSet<SharedCacheResourceReference>();
      for (SharedCacheResourceReference r : refs) {
        if (!appChecker.isApplicationActive(r.getAppId())) {
          // application in resource reference is dead, it is safe to remove the
          // reference
          refsToRemove.add(r);
        }
      }
      if (refsToRemove.size() > 0) {
        store.removeResourceReferences(key, refsToRemove, false);
      }
    }
  }

  /**
   * A status indicating what happened with the processing of a given cache
   * resource.
   */
  private enum ResourceStatus {
    INIT,
    /** Resource was successfully processed, but not deleted **/
    PROCESSED,
    /** Resource was successfully deleted **/
    DELETED,
    /** The cleaner task ran into an error while processing the resource **/
    ERROR;
  }
}
