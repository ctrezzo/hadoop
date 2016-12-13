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
package org.apache.hadoop.mapreduce;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.filecache.ClientDistributedCacheManager;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.SharedCacheClient;
import org.apache.hadoop.yarn.exceptions.YarnException;

import com.google.common.annotations.VisibleForTesting;

/**
 * This class is responsible for uploading resources from the client to HDFS
 * that are associated with a MapReduce job.
 */
@Private
@Unstable
class JobResourceUploader {
  protected static final Log LOG = LogFactory.getLog(JobResourceUploader.class);
  private final boolean useWildcard;
  private final FileSystem jtFs;
  private SharedCacheClient scClient = null;
  private SharedCacheConfig scConfig = new SharedCacheConfig();
  private ApplicationId appId = null;

  JobResourceUploader(FileSystem submitFs, boolean useWildcard) {
    this.jtFs = submitFs;
    this.useWildcard = useWildcard;
  }

  private void initSharedCache(JobID jobid, Configuration conf) {
    this.scConfig.init(conf);
    if (this.scConfig.isSharedCacheEnabled()) {
      this.scClient = createSharedCacheClient(conf);
      appId = ApplicationId.fromString(jobid.getJtIdentifier());
    }
  }

  private void stopSharedCache() {
    if (scClient != null) {
      scClient.stop();
      scClient = null;
    }
  }

  /**
   * Create, initialize and start a new shared cache client.
   */
  @VisibleForTesting
  protected SharedCacheClient createSharedCacheClient(Configuration conf) {
    SharedCacheClient scc = SharedCacheClient.createSharedCacheClient();
    scc.init(conf);
    scc.start();
    return scc;
  }

  /**
   * Upload and configure files, libjars, jobjars, and archives pertaining to
   * the passed job.
   * <p>
   * This client will use the shared cache for libjars, files, archives and
   * jobjars if it is enabled. When shared cache is enabled, it will try to use
   * the shared cache and fall back to the default behavior when the scm isn't
   * available.
   * <p>
   * 1. For the resources that have been successfully shared, we will continue
   * to use them in a shared fashion.
   * <p>
   * 2. For the resources that weren't in the cache and need to be uploaded by
   * NM, we won't ask NM to upload them.
   *
   * @param job the job containing the files to be uploaded
   * @param submitJobDir the submission directory of the job
   * @throws IOException
   */
  public void uploadResources(Job job, Path submitJobDir) throws IOException {
    try {
      initSharedCache(job.getJobID(), job.getConfiguration());
      uploadResourcesInternal(job, submitJobDir);
    } finally {
      stopSharedCache();
    }
  }

  private void uploadResourcesInternal(Job job, Path submitJobDir)
      throws IOException {
    Configuration conf = job.getConfiguration();
    short replication =
        (short) conf.getInt(Job.SUBMIT_REPLICATION,
            Job.DEFAULT_SUBMIT_REPLICATION);

    if (!(conf.getBoolean(Job.USED_GENERIC_PARSER, false))) {
      LOG.warn("Hadoop command-line option parsing not performed. "
          + "Implement the Tool interface and execute your application "
          + "with ToolRunner to remedy this.");
    }

    //
    // Figure out what fs the JobTracker is using. Copy the
    // job to it, under a temporary name. This allows DFS to work,
    // and under the local fs also provides UNIX-like object loading
    // semantics. (that is, if the job file is deleted right after
    // submission, we can still run the submission to completion)
    //

    // Create a number of filenames in the JobTracker's fs namespace
    LOG.debug("default FileSystem: " + jtFs.getUri());
    if (jtFs.exists(submitJobDir)) {
      throw new IOException("Not submitting job. Job directory " + submitJobDir
          + " already exists!! This is unexpected.Please check what's there in"
          + " that directory");
    }
    // Create the submission directory for the MapReduce job.
    submitJobDir = jtFs.makeQualified(submitJobDir);
    submitJobDir = new Path(submitJobDir.toUri().getPath());
    FsPermission mapredSysPerms =
        new FsPermission(JobSubmissionFiles.JOB_DIR_PERMISSION);
    FileSystem.mkdirs(jtFs, submitJobDir, mapredSysPerms);

    Collection<String> files = conf.getStringCollection("tmpfiles");
    Collection<String> libjars = conf.getStringCollection("tmpjars");
    Collection<String> archives = conf.getStringCollection("tmparchives");
    Collection<String> dcFiles =
        conf.getStringCollection(MRJobConfig.CACHE_FILES);
    Collection<String> dcArchives =
        conf.getStringCollection(MRJobConfig.CACHE_ARCHIVES);
    String jobJar = job.getJar();

    // Merge resources that have already been specified for the shared cache
    // via conf.
    files.addAll(conf.getStringCollection(MRJobConfig.FILES_FOR_SHARED_CACHE));
    libjars.addAll(conf.getStringCollection(
            MRJobConfig.FILES_FOR_CLASSPATH_AND_SHARED_CACHE));
    archives.addAll(conf
        .getStringCollection(MRJobConfig.ARCHIVES_FOR_SHARED_CACHE));


    Map<URI, FileStatus> statCache = new HashMap<URI, FileStatus>();
    checkLocalizationLimits(conf, files, libjars, archives, jobJar, dcFiles,
        dcArchives, statCache);

    Map<String, Boolean> fileSCUploadPolicies = new HashMap<String, Boolean>();
    Map<String, Boolean> archiveSCUploadPolicies =
        new HashMap<String, Boolean>();

    uploadFiles(job, files, submitJobDir, mapredSysPerms, replication,
        fileSCUploadPolicies, statCache);
    uploadLibJars(job, libjars, submitJobDir, mapredSysPerms, replication,
        fileSCUploadPolicies, statCache);
    uploadArchives(job, archives, submitJobDir, mapredSysPerms, replication,
        archiveSCUploadPolicies, statCache);
    uploadJobJar(job, jobJar, submitJobDir, replication);
    addLog4jToDistributedCache(job, submitJobDir);

    // Note, we do not consider resources in the distributed cache for the
    // shared cache at this time. Only resources specified via the
    // GenericOptionsParser or the jobjar.
    Job.setFileSharedCacheUploadPolicies(conf, fileSCUploadPolicies);
    Job.setArchiveSharedCacheUploadPolicies(conf, archiveSCUploadPolicies);

    // set the timestamps of the archives and files
    // set the public/private visibility of the archives and files
    ClientDistributedCacheManager.determineTimestampsAndCacheVisibilities(conf,
        statCache);
    // get DelegationToken for cached file
    ClientDistributedCacheManager.getDelegationTokens(conf,
        job.getCredentials());
  }

  private void uploadFiles(Job job, Collection<String> files,
      Path submitJobDir, FsPermission mapredSysPerms, short submitReplication,
      Map<String, Boolean> fileSCUploadPolicies,
      Map<URI, FileStatus> statCache) throws IOException {
    Configuration conf = job.getConfiguration();
    Path filesDir = JobSubmissionFiles.getJobDistCacheFiles(submitJobDir);
    if (!files.isEmpty()) {
      FileSystem.mkdirs(jtFs, filesDir, mapredSysPerms);
      for (String tmpFile : files) {
        URI tmpURI = null;
        try {
          tmpURI = new URI(tmpFile);
        } catch (URISyntaxException e) {
          throw new IllegalArgumentException(e);
        }
        Path tmp = new Path(tmpURI);
        Path newPath = null;
        boolean uploadToSharedCache = false;
        if (scConfig.isSharedCacheFilesEnabled()) {
          if (getFileStatus(statCache, conf, tmp).isDirectory()) {
            LOG.warn("Shared cache does not support directories."
                + " Will not upload " + tmp + " to the shared cache.");
          } else {
            newPath = useSharedCache(tmp, conf);
            if (newPath == null) {
              uploadToSharedCache = true;
            }
          }
        }

        if (newPath == null) {
          newPath = copyRemoteFiles(filesDir, tmp, conf, submitReplication);
        }

        try {
          URI pathURI = getPathURI(newPath, tmpURI.getFragment());
          job.addCacheFile(pathURI);
          if (scConfig.isSharedCacheFilesEnabled()) {
            // handle a path that has been specified multiple times
            Boolean previousPolicy =
                fileSCUploadPolicies.get(pathURI.toString());
            if (previousPolicy == null || previousPolicy == false) {
              fileSCUploadPolicies.put(pathURI.toString(), uploadToSharedCache);
            }
          }
        } catch (URISyntaxException ue) {
          // should not throw a uri exception
          throw new IOException("Failed to create uri for " + tmpFile, ue);
        }
      }
    }
  }

  private void uploadLibJars(Job job, Collection<String> libjars,
      Path submitJobDir, FsPermission mapredSysPerms, short submitReplication,
      Map<String, Boolean> fileSCUploadPolicies, Map<URI, FileStatus> statCache)
      throws IOException {
    Configuration conf = job.getConfiguration();
    Path libjarsDir = JobSubmissionFiles.getJobDistCacheLibjars(submitJobDir);
    if (!libjars.isEmpty()) {
      FileSystem.mkdirs(jtFs, libjarsDir, mapredSysPerms);
      boolean addIndividualToDistributedCache = !useWildcard;
      if (scConfig.isSharedCacheLibjarsEnabled()) {
        // Shared cache does not support wildcards for libjars directory yet
        addIndividualToDistributedCache = true;
      }
      for (String tmpjars : libjars) {
        Path tmp = new Path(tmpjars);
        Path newPath = null;
        boolean uploadToSharedCache = false;
        if (scConfig.isSharedCacheLibjarsEnabled()) {
          if (getFileStatus(statCache, conf, tmp).isDirectory()) {
            LOG.warn("Shared cache does not support directories."
                + " Will not upload " + tmp + " to the shared cache.");
          } else {
            newPath = useSharedCache(tmp, conf);
            if (newPath == null) {
              uploadToSharedCache = true;
            }
          }
        }

        if (newPath == null) {
          newPath = copyRemoteFiles(libjarsDir, tmp, conf, submitReplication);
        }

        // Add each file to the classpath
        job.addFileToClassPath(newPath);
        if (addIndividualToDistributedCache) {
          job.addCacheFile(newPath.toUri());
        }
        if (scConfig.isSharedCacheLibjarsEnabled()) {
          // handle a path that has been specified multiple times
          String pathUriString = newPath.toUri().toString();
          Boolean previousPolicy = fileSCUploadPolicies.get(pathUriString);
          if (previousPolicy == null || previousPolicy == false) {
            fileSCUploadPolicies.put(pathUriString, uploadToSharedCache);
          }
        }
      }

      if (!addIndividualToDistributedCache) {
        // Add the whole directory to the cache
        Path libJarsDirWildcard =
            jtFs.makeQualified(new Path(libjarsDir, DistributedCache.WILDCARD));

        DistributedCache.addCacheFile(libJarsDirWildcard.toUri(), conf);
      }
    }
  }

  private void uploadArchives(Job job, Collection<String> archives,
      Path submitJobDir, FsPermission mapredSysPerms, short submitReplication,
      Map<String, Boolean> archiveSCUploadPolicies,
      Map<URI, FileStatus> statCache)
      throws IOException {
    Configuration conf = job.getConfiguration();
    Path archivesDir = JobSubmissionFiles.getJobDistCacheArchives(submitJobDir);
    if (!archives.isEmpty()) {
      FileSystem.mkdirs(jtFs, archivesDir, mapredSysPerms);
      for (String tmpArchives : archives) {
        URI tmpURI;
        try {
          tmpURI = new URI(tmpArchives);
        } catch (URISyntaxException e) {
          throw new IllegalArgumentException(e);
        }
        Path tmp = new Path(tmpURI);
        Path newPath = null;
        boolean uploadToSharedCache = false;
        if (scConfig.isSharedCacheArchivesEnabled()) {
          if (getFileStatus(statCache, conf, tmp).isDirectory()) {
            LOG.warn("Shared cache does not support directories."
                + " Will not upload " + tmp + " to the shared cache.");
          } else {
            newPath = useSharedCache(tmp, conf);
            if (newPath == null) {
              uploadToSharedCache = true;
            }
          }
        }

        if (newPath == null) {
          newPath = copyRemoteFiles(archivesDir, tmp, conf, submitReplication);
        }
        try {
          URI pathURI = getPathURI(newPath, tmpURI.getFragment());
          job.addCacheArchive(pathURI);
          if (scConfig.isSharedCacheArchivesEnabled()) {
            // handle a path that has been specified multiple times
            Boolean previousPolicy =
                archiveSCUploadPolicies.get(pathURI.toString());
            if (previousPolicy == null || previousPolicy == false) {
              archiveSCUploadPolicies.put(pathURI.toString(),
                  uploadToSharedCache);
            }
          }
        } catch (URISyntaxException ue) {
          // should not throw an uri excpetion
          throw new IOException("Failed to create uri for " + tmpArchives, ue);
        }
      }
    }
  }

  private void uploadJobJar(Job job, String jobJar, Path submitJobDir,
      short submitReplication) throws IOException {
    Configuration conf = job.getConfiguration();
    if (jobJar != null) { // copy jar to JobTracker's fs
      // use jar name if job is not named.
      if ("".equals(job.getJobName())) {
        job.setJobName(new Path(jobJar).getName());
      }
      if (scConfig.isSharedCacheJobjarEnabled()) {
        // We want the jobjar to be localized with public visibility on the
        // node manager so it can be shared.
        conf.setBoolean(MRJobConfig.JOBJAR_VISIBILITY, true);
      }
      Path jobJarPath = new Path(jobJar);
      URI jobJarURI = jobJarPath.toUri();
      Path newJarPath = null;
      boolean uploadToSharedCache = false;
      if (jobJarURI.getScheme() == null ||
          jobJarURI.getScheme().equals("file")) {
        // job jar is on the local file system
        if (scConfig.isSharedCacheJobjarEnabled()) {
          // We must have a qualified path for the shared cache client. We can
          // assume this is for the local filesystem
          if (jobJarPath.toUri().getScheme() == null
              || jobJarPath.toUri().getAuthority() == null) {
            jobJarPath = FileSystem.getLocal(conf).makeQualified(jobJarPath);
          }
          newJarPath = useSharedCache(jobJarPath, conf);
          if (newJarPath == null) {
            uploadToSharedCache = true;
          }
        }
        if (newJarPath == null) {
          newJarPath = JobSubmissionFiles.getJobJar(submitJobDir);
          copyJar(jobJarPath, newJarPath, submitReplication);
        }
      } else {
        // job jar is in a remote file system
        if (scConfig.isSharedCacheJobjarEnabled()) {
          newJarPath = useSharedCache(jobJarPath, conf);
          if (newJarPath == null) {
            uploadToSharedCache = true;
            newJarPath = jobJarPath;
          }
        } else {
          // we don't need to upload the jobjar to the staging directory because
          // it is already in an accessible place
          newJarPath = jobJarPath;
        }
      }
      job.setJar(newJarPath.toString());
      if (scConfig.isSharedCacheJobjarEnabled()) {
        conf.setBoolean(MRJobConfig.JOBJAR_SHARED_CACHE_UPLOAD_POLICY,
            uploadToSharedCache);
      }
    } else {
      LOG.warn("No job jar file set.  User classes may not be found. "
          + "See Job or Job#setJar(String).");
    }
  }

  /**
   * Verify that the resources this job is going to localize are within the
   * localization limits. We count all resources towards these limits regardless
   * of where they are coming from (i.e. local, distributed cache, or shared
   * cache).
   */
  @VisibleForTesting
  void checkLocalizationLimits(Configuration conf, Collection<String> files,
      Collection<String> libjars, Collection<String> archives, String jobJar,
      Collection<String> dcFiles, Collection<String> dcArchives,
      Map<URI, FileStatus> statCache) throws IOException {

    LimitChecker limitChecker = new LimitChecker(conf);
    if (!limitChecker.hasLimits()) {
      // there are no limits set, so we are done.
      return;
    }

    for (String path : dcFiles) {
      explorePath(conf, new Path(path), limitChecker, statCache);
    }

    for (String path : dcArchives) {
      explorePath(conf, new Path(path), limitChecker, statCache);
    }

    for (String path : files) {
      explorePath(conf, new Path(path), limitChecker, statCache);
    }

    for (String path : libjars) {
      explorePath(conf, new Path(path), limitChecker, statCache);
    }

    for (String path : archives) {
      explorePath(conf, new Path(path), limitChecker, statCache);
    }

    if (jobJar != null) {
      explorePath(conf, new Path(jobJar), limitChecker, statCache);
    }
  }

  @VisibleForTesting
  protected static final String MAX_RESOURCE_ERR_MSG =
      "This job has exceeded the maximum number of submitted resources";
  @VisibleForTesting
  protected static final String MAX_TOTAL_RESOURCE_MB_ERR_MSG =
      "This job has exceeded the maximum size of submitted resources";
  @VisibleForTesting
  protected static final String MAX_SINGLE_RESOURCE_MB_ERR_MSG =
      "This job has exceeded the maximum size of a single submitted resource";

  private static class LimitChecker {
    LimitChecker(Configuration conf) {
      this.maxNumOfResources =
          conf.getInt(MRJobConfig.MAX_RESOURCES,
              MRJobConfig.MAX_RESOURCES_DEFAULT);
      this.maxSizeMB =
          conf.getLong(MRJobConfig.MAX_RESOURCES_MB,
              MRJobConfig.MAX_RESOURCES_MB_DEFAULT);
      this.maxSizeOfResourceMB =
          conf.getLong(MRJobConfig.MAX_SINGLE_RESOURCE_MB,
              MRJobConfig.MAX_SINGLE_RESOURCE_MB_DEFAULT);
      this.totalConfigSizeBytes = maxSizeMB * 1024 * 1024;
      this.totalConfigSizeOfResourceBytes = maxSizeOfResourceMB * 1024 * 1024;
    }

    private long totalSizeBytes = 0;
    private int totalNumberOfResources = 0;
    private long currentMaxSizeOfFileBytes = 0;
    private final long maxSizeMB;
    private final int maxNumOfResources;
    private final long maxSizeOfResourceMB;
    private final long totalConfigSizeBytes;
    private final long totalConfigSizeOfResourceBytes;

    private boolean hasLimits() {
      return maxNumOfResources > 0 || maxSizeMB > 0 || maxSizeOfResourceMB > 0;
    }

    private void addFile(Path p, long fileSizeBytes) throws IOException {
      totalNumberOfResources++;
      totalSizeBytes += fileSizeBytes;
      if (fileSizeBytes > currentMaxSizeOfFileBytes) {
        currentMaxSizeOfFileBytes = fileSizeBytes;
      }

      if (totalConfigSizeBytes > 0 && totalSizeBytes > totalConfigSizeBytes) {
        throw new IOException(MAX_TOTAL_RESOURCE_MB_ERR_MSG + " (Max: "
            + maxSizeMB + "MB).");
      }

      if (maxNumOfResources > 0 &&
          totalNumberOfResources > maxNumOfResources) {
        throw new IOException(MAX_RESOURCE_ERR_MSG + " (Max: "
            + maxNumOfResources + ").");
      }

      if (totalConfigSizeOfResourceBytes > 0
          && currentMaxSizeOfFileBytes > totalConfigSizeOfResourceBytes) {
        throw new IOException(MAX_SINGLE_RESOURCE_MB_ERR_MSG + " (Max: "
            + maxSizeOfResourceMB + "MB, Violating resource: " + p + ").");
      }
    }
  }

  /**
   * Recursively explore the given path and enforce the limits for resource
   * localization. This method assumes that there are no symlinks in the
   * directory structure.
   */
  private void explorePath(Configuration job, Path p,
      LimitChecker limitChecker, Map<URI, FileStatus> statCache)
      throws IOException {
    Path pathWithScheme = p;
    if (!pathWithScheme.toUri().isAbsolute()) {
      // the path does not have a scheme, so we assume it is a path from the
      // local filesystem
      FileSystem localFs = FileSystem.getLocal(job);
      pathWithScheme = localFs.makeQualified(p);
    }
    FileStatus status = getFileStatus(statCache, job, pathWithScheme);
    if (status.isDirectory()) {
      FileStatus[] statusArray =
          pathWithScheme.getFileSystem(job).listStatus(pathWithScheme);
      for (FileStatus s : statusArray) {
        explorePath(job, s.getPath(), limitChecker, statCache);
      }
    } else {
      limitChecker.addFile(pathWithScheme, status.getLen());
    }
  }

  @VisibleForTesting
  FileStatus getFileStatus(Map<URI, FileStatus> statCache,
      Configuration job, Path p) throws IOException {
    URI u = p.toUri();
    FileStatus status = statCache.get(u);
    if (status == null) {
      status = p.getFileSystem(job).getFileStatus(p);
      statCache.put(u, status);
    }
    return status;
  }

  // copies a file to the jobtracker filesystem and returns the path where it
  // was copied to
  private Path copyRemoteFiles(Path parentDir, Path originalPath,
      Configuration conf, short replication) throws IOException {
    // check if we do not need to copy the files
    // is jt using the same file system.
    // just checking for uri strings... doing no dns lookups
    // to see if the filesystems are the same. This is not optimal.
    // but avoids name resolution.

    FileSystem remoteFs = null;
    remoteFs = originalPath.getFileSystem(conf);
    if (FileUtil.compareFs(remoteFs, jtFs)) {
      return originalPath;
    }
    // this might have name collisions. copy will throw an exception
    // parse the original path to create new path
    Path newPath = new Path(parentDir, originalPath.getName());
    FileUtil.copy(remoteFs, originalPath, jtFs, newPath, false, conf);
    jtFs.setReplication(newPath, replication);
    return newPath;
  }

  /**
   * Checksum a local resource file and call use for that resource with the scm.
   */
  private Path useSharedCache(Path sourceFile, Configuration conf)
      throws IOException {
    if (scClient == null) {
      return null;
    }
    // If for whatever reason, we can't even calculate checksum for
    // a resource, something is really wrong with the file system;
    // even non-SCM approach won't work. Let us just throw the exception.
    String checksum = scClient.getFileChecksum(sourceFile);
    Path path = null;
    try {
      path = scClient.use(this.appId, checksum);
    } catch (YarnException e) {
      LOG.warn("Error trying to contact the shared cache manager,"
          + " disabling the SCMClient for the rest of this job submission", e);
      /*
       * If we fail to contact the SCM, we do not use it for the rest of this
       * JobResourceUploader's life. This prevents us from having to timeout
       * each time we try to upload a file while the SCM is unavailable. Instead
       * we timeout/error the first time and quickly revert to the default
       * behavior without the shared cache. We do this by stopping the shared
       * cache client and setting it to null.
       */
      stopSharedCache();
    }
    return path;
  }

  private void copyJar(Path originalJarPath, Path submitJarFile,
      short replication) throws IOException {
    jtFs.copyFromLocalFile(originalJarPath, submitJarFile);
    jtFs.setReplication(submitJarFile, replication);
    jtFs.setPermission(submitJarFile, new FsPermission(
        JobSubmissionFiles.JOB_FILE_PERMISSION));
  }

  private void addLog4jToDistributedCache(Job job, Path jobSubmitDir)
      throws IOException {
    Configuration conf = job.getConfiguration();
    String log4jPropertyFile =
        conf.get(MRJobConfig.MAPREDUCE_JOB_LOG4J_PROPERTIES_FILE, "");
    if (!log4jPropertyFile.isEmpty()) {
      short replication = (short) conf.getInt(Job.SUBMIT_REPLICATION, 10);
      copyLog4jPropertyFile(job, jobSubmitDir, replication);
    }
  }

  private URI getPathURI(Path destPath, String fragment)
      throws URISyntaxException {
    URI pathURI = destPath.toUri();
    if (pathURI.getFragment() == null) {
      if (fragment == null) {
        pathURI = new URI(pathURI.toString() + "#" + destPath.getName());
      } else {
        pathURI = new URI(pathURI.toString() + "#" + fragment);
      }
    }
    return pathURI;
  }

  // copy user specified log4j.property file in local
  // to HDFS with putting on distributed cache and adding its parent directory
  // to classpath.
  @SuppressWarnings("deprecation")
  private void copyLog4jPropertyFile(Job job, Path submitJobDir,
      short replication) throws IOException {
    Configuration conf = job.getConfiguration();

    String file =
        validateFilePath(
            conf.get(MRJobConfig.MAPREDUCE_JOB_LOG4J_PROPERTIES_FILE), conf);
    LOG.debug("default FileSystem: " + jtFs.getUri());
    FsPermission mapredSysPerms =
        new FsPermission(JobSubmissionFiles.JOB_DIR_PERMISSION);
    try {
      jtFs.getFileStatus(submitJobDir);
    } catch (FileNotFoundException e) {
      throw new IOException("Cannot find job submission directory! "
          + "It should just be created, so something wrong here.", e);
    }

    Path fileDir = JobSubmissionFiles.getJobLog4jFile(submitJobDir);

    // first copy local log4j.properties file to HDFS under submitJobDir
    if (file != null) {
      FileSystem.mkdirs(jtFs, fileDir, mapredSysPerms);
      URI tmpURI = null;
      try {
        tmpURI = new URI(file);
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException(e);
      }
      Path tmp = new Path(tmpURI);
      Path newPath = copyRemoteFiles(fileDir, tmp, conf, replication);
      DistributedCache.addFileToClassPath(new Path(newPath.toUri().getPath()),
          conf);
    }
  }

  /**
   * takes input as a path string for file and verifies if it exist. It defaults
   * for file:/// if the files specified do not have a scheme. it returns the
   * paths uri converted defaulting to file:///. So an input of /home/user/file1
   * would return file:///home/user/file1
   * 
   * @param file
   * @param conf
   * @return
   */
  private String validateFilePath(String file, Configuration conf)
      throws IOException {
    if (file == null) {
      return null;
    }
    if (file.isEmpty()) {
      throw new IllegalArgumentException("File name can't be empty string");
    }
    String finalPath;
    URI pathURI;
    try {
      pathURI = new URI(file);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
    Path path = new Path(pathURI);
    FileSystem localFs = FileSystem.getLocal(conf);
    if (pathURI.getScheme() == null) {
      // default to the local file system
      // check if the file exists or not first
      localFs.getFileStatus(path);
      finalPath =
          path.makeQualified(localFs.getUri(), localFs.getWorkingDirectory())
              .toString();
    } else {
      // check if the file exists in this file system
      // we need to recreate this filesystem object to copy
      // these files to the file system ResourceManager is running
      // on.
      FileSystem fs = path.getFileSystem(conf);
      fs.getFileStatus(path);
      finalPath =
          path.makeQualified(fs.getUri(), fs.getWorkingDirectory()).toString();
    }
    return finalPath;
  }
}
