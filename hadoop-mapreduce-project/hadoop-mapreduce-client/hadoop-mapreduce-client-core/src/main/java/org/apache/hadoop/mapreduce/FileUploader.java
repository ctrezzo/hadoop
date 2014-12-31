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
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
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

@InterfaceAudience.Private
@InterfaceStability.Unstable
class FileUploader {
  protected static final Log LOG = LogFactory.getLog(FileUploader.class);
  private FileSystem jtFs;
  private SharedCacheConfig scConfig = new SharedCacheConfig();
  private SharedCacheClient scClient = null;
  private ApplicationId appId = null;

  /**
   * If we fail to contact the SCM, we do not use it for the rest of this
   * FileUploaders life. This prevents us from having to timeout each time we
   * try to upload a file while the SCM is unavailable. Instead we timeout/error
   * the first time and quickly revert to the default behavior without the
   * shared cache.
   */
  private volatile boolean scmAvailable = false;

  FileUploader(FileSystem submitFs) {
    this.jtFs = submitFs;
  }

  private void initSharedCache(JobID jobid, Configuration conf) {
    this.scConfig.init(conf);
    if (this.scConfig.isSharedCacheEnabled()) {
      this.scClient = createSharedCacheClient(conf);
      appId = jobid.getAppId();
    }
  }

  private void stopSharedCache() {
    if (scClient != null) {
      scClient.stop();
    }
  }

  // make it protected so that test code can overload the method
  // to provide test or mock SharedCacheClient
  protected SharedCacheClient createSharedCacheClient(Configuration conf) {
    SharedCacheClient scClient = SharedCacheClient.createSharedCacheClient();
    scClient.init(conf);
    scClient.start();
    scmAvailable = true;
    return scClient;
  }

  private boolean isScmAvailable() {
    return this.scmAvailable;
  }

  private boolean isSharedCacheFilesEnabled() {
    return (scConfig.isSharedCacheFilesEnabled() && isScmAvailable());
  }

  private boolean isSharedCacheLibjarsEnabled() {
    return (scConfig.isSharedCacheLibjarsEnabled() && isScmAvailable());
  }

  private boolean isSharedCacheArchivesEnabled() {
    return (scConfig.isSharedCacheArchivesEnabled() && isScmAvailable());
  }

  private boolean isSharedCacheJobjarEnabled() {
    return (scConfig.isSharedCacheJobjarEnabled() && isScmAvailable());
  }

  private Path useSharedCache(Path sourceFile, Configuration conf,
      boolean createSymLinkIfNecessary) throws IOException {
    // If for whatever reason, we can't even calculate checksum for
    // local resource, something is really wrong with the file system;
    // even non-SCM approach won't work. Let us just throw the exception.
    String checksum = scClient.getFileChecksum(sourceFile);
    Path path = null;
    try {
      path = scClient.use(this.appId, checksum);
    } catch (YarnException e) {
      LOG.warn("Error trying to contact the shared cache manager,"
          + " disabling the SCMClient for the rest of this job submission", e);
      scmAvailable = false;
    }
    if (path != null) {
      addChecksum(checksum, conf);
      // Symlink is used to address the following scenario.
      // There are two jars A.jar and B.jar. They are available in SCM,
      // but are stored under the same name, e.g., checksum1/job.jar",
      // checksum2/job.jar. Without explicit symlink, "job.jar" will be used
      // as the symlink name and thus cause one symlink overrides the other.
      // With symlink, the resource URL will be checksum1/job.jar#A.jar and
      // checksum2/job.jar#B.jar. YARN will use the explicit symlinks during
      // localization, e.g.,
      // $(container_id_dir)/A.jar -> yarn/local/filecache/10/job.jar
      // $(container_id_dir)/B.jar -> yarn/local/filecache/11/job.jar
      String symlink = (sourceFile.toUri().getFragment() == null) ?
          sourceFile.getName() : sourceFile.toUri().getFragment();
      if (createSymLinkIfNecessary && !path.getName().equals(symlink)) {
        try {
          path = new Path(getPathURI(path, symlink));
        } catch(URISyntaxException ue) {
          //should not throw a uri exception
          throw new IOException("Failed to create uri for " + path, ue);
        }
      }
    }
    return path;
  }

  /**
   * Add a checksum to the conf. MR master will release the resource
   * later after the job succeeds.
   * @param checksum The checksum
   * @param conf Configuration to add the cache to
   */
  private static void addChecksum(String checksum, Configuration conf) {
    String checksums = conf.get(MRJobConfig.SHARED_CACHE_CHECKSUMS);
    conf.set(MRJobConfig.SHARED_CACHE_CHECKSUMS, checksums == null ?
        checksum : checksums + "," + checksum);
  }

  // Merge the files passed from CLI with those via DistributedCache APIs
  // addCacheFileShared, addFileToClassPathShared, addCacheArchiveToShared
  private String getFiles(Configuration conf, String cliConfigName,
      String sharedCacheConfigName) {
    String files = conf.get(cliConfigName);
    String dcFiles = conf.get(sharedCacheConfigName);
    if (files != null && dcFiles != null) {
      files = files + "," + dcFiles;
    } else if (files == null) {
      files = dcFiles;
    }
    return files;
  }

  /**
   * Upload and configure files, libjars, jobjars, and archives pertaining to
   * the passed job. This client will use the shared cache for libjars and
   * jobjars if it is enabled.
   * When shared cache is enabled, it will try to use the shared cache and fall back
   * to the default behavior when shared cache isn't available.
   * 1.For the resources that have been successfully shared,
   *   we will continue to use them in a shared fashion.
   * 2.For the resources that weren't in the cache and need to be uploaded by NM,
   *   we won't ask NM to upload them.   *
   * @param job the job containing the files to be uploaded
   * @param submitJobDir the submission directory of the job
   * @throws IOException
   */
  public void uploadFiles(Job job, Path submitJobDir)
      throws IOException {
    Configuration conf = job.getConfiguration();
    short replication =
        (short) conf.getInt(Job.SUBMIT_REPLICATION,
            MRJobConfig.SUBMIT_FILE_REPLICATION_DEFAULT);

    if (!(conf.getBoolean(Job.USED_GENERIC_PARSER, false))) {
      LOG.warn("Hadoop command-line option parsing not performed. " +
               "Implement the Tool interface and execute your application " +
               "with ToolRunner to remedy this.");
    }

    try {
      initSharedCache(job.getJobID(), conf);

      // get all the command line arguments passed in by the user conf
      // as well as DistributedCache APIs
      String files =
          getFiles(conf, "tmpfiles", MRJobConfig.FILES_FOR_SHARED_CACHE);
      String libjars =
          getFiles(conf, "tmpjars",
              MRJobConfig.FILES_FOR_CLASSPATH_AND_SHARED_CACHE);
      String archives =
          getFiles(conf, "tmparchives", MRJobConfig.ARCHIVES_FOR_SHARED_CACHE);
      String jobJar = job.getJar();

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
        throw new IOException(
            "Not submitting job. Job directory "
                + submitJobDir
                + " already exists!! This is unexpected.Please check what's there in"
                + " that directory");
      }

      submitJobDir = jtFs.makeQualified(submitJobDir);
      submitJobDir = new Path(submitJobDir.toUri().getPath());
      FsPermission mapredSysPerms =
          new FsPermission(JobSubmissionFiles.JOB_DIR_PERMISSION);
      FileSystem.mkdirs(jtFs, submitJobDir, mapredSysPerms);
      Path filesDir = JobSubmissionFiles.getJobDistCacheFiles(submitJobDir);
      Path archivesDir =
          JobSubmissionFiles.getJobDistCacheArchives(submitJobDir);
      // add all the command line files/ jars and archive
      // first copy them to jobtrackers filesystem

      // We need to account for the distributed cache files applications specify
      // via
      // Job or Distributed Cache APIs such as addCacheFile. These files are
      // already in HDFS.
      // We don't ask YARN to upload those files to shared cache.
      int numOfFilesSCUploadPolicies =
          job.getCacheFiles() != null ? job.getCacheFiles().length : 0;
      int indexOfFilesSCUploadPolicies = numOfFilesSCUploadPolicies;
      int numOfArchivesSCUploadPolicies =
          job.getCacheArchives() != null ? job.getCacheArchives().length : 0;
      int indexOfArchivesSCUploadPolicies = numOfArchivesSCUploadPolicies;

      String[] fileArr = null;
      if (files != null) {
        FileSystem.mkdirs(jtFs, filesDir, mapredSysPerms);
        fileArr = files.split(",");
        numOfFilesSCUploadPolicies += fileArr.length;
      }

      String[] libjarsArr = null;
      if (libjars != null) {
        libjarsArr = libjars.split(",");
        numOfFilesSCUploadPolicies += libjarsArr.length;
      }

      String[] archivesArr = null;
      if (archives != null) {
        FileSystem.mkdirs(jtFs, archivesDir, mapredSysPerms);
        archivesArr = archives.split(",");
        numOfArchivesSCUploadPolicies += archivesArr.length;
      }

      boolean[] filesSCUploadPolicies = new boolean[numOfFilesSCUploadPolicies];
      boolean[] archivesSCUploadPolicies =
          new boolean[numOfArchivesSCUploadPolicies];

      if (files != null) {
        for (String tmpFile : fileArr) {
          URI tmpURI = null;
          try {
            tmpURI = new URI(tmpFile);
          } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
          }
          Path tmp = new Path(tmpURI);
          Path newPath = null;
          if (isSharedCacheFilesEnabled()) {
            newPath = useSharedCache(tmp, conf, true);
          }

          // need to inform NM to upload the file to shared cache.
          if (newPath == null && isSharedCacheFilesEnabled()) {
            filesSCUploadPolicies[indexOfFilesSCUploadPolicies] = true;
          }
          indexOfFilesSCUploadPolicies++;

          if (newPath == null) {
            newPath = copyRemoteFiles(filesDir, tmp, conf, replication);
          }
          try {
            URI pathURI = getPathURI(newPath, tmpURI.getFragment());
            job.addCacheFile(pathURI);
          } catch (URISyntaxException ue) {
            // should not throw a uri exception
            throw new IOException("Failed to create uri for " + tmpFile, ue);
          }
        }
      }

      if (libjars != null) {
        for (String tmpjars : libjarsArr) {
          Path tmp = new Path(tmpjars);
          Path newPath = null;
          if (isSharedCacheLibjarsEnabled()) {
            newPath = useSharedCache(tmp, conf, true);
          }

          // need to inform NM to upload the file to shared cache.
          if (newPath == null && isSharedCacheLibjarsEnabled()) {
            filesSCUploadPolicies[indexOfFilesSCUploadPolicies] = true;
          }
          indexOfFilesSCUploadPolicies++;

          if (newPath == null) {
            Path libjarsDir =
                JobSubmissionFiles.getJobDistCacheLibjars(submitJobDir);
            FileSystem.mkdirs(jtFs, libjarsDir, mapredSysPerms);
            newPath = copyRemoteFiles(libjarsDir, tmp, conf, replication);
          }

          // In order to support symlink for libjars, we pass the full URL
          // to DistributedCache.addFileToClassPath.
          // DistributedCache.addFileToClassPath will set the classpath using
          // the Path part without fragment, e.g. file.toUri().getPath().
          // DistributedCache.addFileToClassPath will call addCacheFile with
          // the full URL with fragment.
          job.addFileToClassPath(newPath);
        }
      }

      if (archives != null) {
        for (String tmpArchives : archivesArr) {
          URI tmpURI;
          try {
            tmpURI = new URI(tmpArchives);
          } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
          }
          Path tmp = new Path(tmpURI);
          Path newPath = null;
          if (isSharedCacheArchivesEnabled()) {
            newPath = useSharedCache(tmp, conf, true);
          }

          // need to inform NM to upload the file to shared cache.
          if (newPath == null && isSharedCacheArchivesEnabled()) {
            archivesSCUploadPolicies[indexOfArchivesSCUploadPolicies] = true;
          }
          indexOfArchivesSCUploadPolicies++;

          if (newPath == null) {
            newPath = copyRemoteFiles(archivesDir, tmp, conf, replication);
          }
          try {
            URI pathURI = getPathURI(newPath, tmpURI.getFragment());
            job.addCacheArchive(pathURI);
          } catch (URISyntaxException ue) {
            // should not throw an uri excpetion
            throw new IOException("Failed to create uri for " + tmpArchives, ue);
          }
        }
      }

      if (jobJar != null) { // copy jar to JobTracker's fs
        // use jar name if job is not named.
        if ("".equals(job.getJobName())) {
          job.setJobName(new Path(jobJar).getName());
        }
        Path jobJarPath = new Path(jobJar);
        URI jobJarURI = jobJarPath.toUri();
        // If the job jar is already in a global fs,
        // we don't need to copy it from local fs
        if (jobJarURI.getScheme() == null
            || jobJarURI.getScheme().equals("file")) {
          Path newJarPath = null;
          if (isSharedCacheJobjarEnabled()) {
            // jobJarPath has the format of "/..." without "file:///" scheme
            // when
            // users submit local job jar from command line.
            // We fix up the scheme so that jobJarPath.getFileSystem(conf) can
            // return the correct file system.
            // Otherwise, scClient won't be able to locate the file when
            // it tries to compute the checksum.
            // This won't break anything given copyJar function below has
            // the assumption the source is a local file.
            // Currently jar file has to be either local or on the same cluster
            // as MR cluster.
            if (jobJarPath.toUri().getScheme() == null
                && jobJarPath.toUri().getAuthority() == null) {
              jobJarPath = FileSystem.getLocal(conf).makeQualified(jobJarPath);
            }
            newJarPath = useSharedCache(jobJarPath, conf, false);
          }

          // Inform NM to upload job jar to shared cache.
          if (newJarPath == null && isSharedCacheJobjarEnabled()) {
            conf.setBoolean(MRJobConfig.JOBJAR_SHARED_CACHE_UPLOAD_POLICY, true);
          } else {
            conf.setBoolean(MRJobConfig.JOBJAR_SHARED_CACHE_UPLOAD_POLICY,
                false);
          }

          if (newJarPath == null) {
            copyJar(jobJarPath, JobSubmissionFiles.getJobJar(submitJobDir),
                replication);
            newJarPath = JobSubmissionFiles.getJobJar(submitJobDir);
          } else {

            // The application wants to use a job jar on the local file system
            // while the job jar is already in shared cache.
            // Set the visibility flag properly so that job jar will be
            // localized
            // to public cache by yarn localizer
            // We don't set it for other scenarios given default value of
            // JOBJAR_VISIBILITY is false

            conf.setBoolean(MRJobConfig.JOBJAR_VISIBILITY, true);
          }
          // set the job jar size if it is being copied
          job.getConfiguration().setLong(MRJobConfig.JAR + ".size",
              getJobJarSize(jobJarPath));
          job.setJar(newJarPath.toString());
        }
      } else {
        LOG.warn("No job jar file set.  User classes may not be found. "
            + "See Job or Job#setJar(String).");
      }

      addLog4jToDistributedCache(job, submitJobDir);

      // if scm fails in the middle, we will set shared cache upload policies
      // for all resources
      // to be false. The resources that are shared successfully via
      // SharedCacheClient.use will
      // continued to be shared.
      if (scClient != null && !isScmAvailable()) {
        conf.setBoolean(MRJobConfig.JOBJAR_SHARED_CACHE_UPLOAD_POLICY, false);
        Job.setFilesSharedCacheUploadPolicies(conf,
            new boolean[filesSCUploadPolicies.length]);
        Job.setArchivesSharedCacheUploadPolicies(conf,
            new boolean[archivesSCUploadPolicies.length]);
      } else {
        Job.setFilesSharedCacheUploadPolicies(conf, filesSCUploadPolicies);
        Job.setArchivesSharedCacheUploadPolicies(conf, archivesSCUploadPolicies);
      }

      // set the timestamps of the archives and files
      // set the public/private visibility of the archives and files
      ClientDistributedCacheManager
          .determineTimestampsAndCacheVisibilities(conf);
      // get DelegationToken for cached file
      ClientDistributedCacheManager.getDelegationTokens(conf,
          job.getCredentials());
    } finally {
      stopSharedCache();
    }
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
    if (compareFs(remoteFs, jtFs)) {
      return originalPath;
    }
    // this might have name collisions. copy will throw an exception
    // parse the original path to create new path
    Path newPath = new Path(parentDir, originalPath.getName());
    FileUtil.copy(remoteFs, originalPath, jtFs, newPath, false, conf);
    jtFs.setReplication(newPath, replication);
    return newPath;
  }
  
  private void copyJar(Path originalJarPath, Path submitJarFile,
      short replication) throws IOException {
    jtFs.copyFromLocalFile(originalJarPath, submitJarFile);
    jtFs.setReplication(submitJarFile, replication);
    jtFs.setPermission(submitJarFile, new FsPermission(
        JobSubmissionFiles.JOB_FILE_PERMISSION));
  }

  private long getJobJarSize(Path jobJarPath) throws IOException {
    FileSystem fs = FileSystem.getLocal(jtFs.getConf());
    FileStatus status = fs.getFileStatus(jobJarPath);
    return status.getLen();
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
  
  /*
   * see if two file systems are the same or not.
   */
  private boolean compareFs(FileSystem srcFs, FileSystem destFs) {
    URI srcUri = srcFs.getUri();
    URI dstUri = destFs.getUri();
    if (srcUri.getScheme() == null) {
      return false;
    }
    if (!srcUri.getScheme().equals(dstUri.getScheme())) {
      return false;
    }
    String srcHost = srcUri.getHost();
    String dstHost = dstUri.getHost();
    if ((srcHost != null) && (dstHost != null)) {
      try {
        srcHost = InetAddress.getByName(srcHost).getCanonicalHostName();
        dstHost = InetAddress.getByName(dstHost).getCanonicalHostName();
      } catch (UnknownHostException ue) {
        return false;
      }
      if (!srcHost.equals(dstHost)) {
        return false;
      }
    } else if (srcHost == null && dstHost != null) {
      return false;
    } else if (srcHost != null && dstHost == null) {
      return false;
    }
    // check for ports
    if (srcUri.getPort() != dstUri.getPort()) {
      return false;
    }
    return true;
  }

  private void addLog4jToDistributedCache(Job job, Path jobSubmitDir)
      throws IOException {
    Configuration conf = job.getConfiguration();
    String log4jPropertyFile =
        conf.get(MRJobConfig.MAPREDUCE_JOB_LOG4J_PROPERTIES_FILE, "");
    if (!log4jPropertyFile.isEmpty()) {
      short replication = (short) conf.getInt(Job.SUBMIT_REPLICATION, 10);
      copyLog4jPropertyFile(job, jobSubmitDir, replication);

      // Set the working directory
      if (job.getWorkingDirectory() == null) {
        job.setWorkingDirectory(jtFs.getWorkingDirectory());
      }
    }
  }

  /**
   * Copy user specified log4j.property file in local to HDFS with putting on
   * distributed cache and adding its parent directory to classpath.
   */
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
    if (!jtFs.exists(submitJobDir)) {
      throw new IOException("Cannot find job submission directory! "
          + "It should just be created, so something wrong here.");
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
   * Takes input as a path string for file and verifies if it exist. It defaults
   * for file:/// if the files specified do not have a scheme. It returns the
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
      if (!localFs.exists(path)) {
        throw new FileNotFoundException("File " + file + " does not exist.");
      }
      finalPath =
          path.makeQualified(localFs.getUri(), localFs.getWorkingDirectory())
              .toString();
    } else {
      // check if the file exists in this file system
      // we need to recreate this filesystem object to copy
      // these files to the file system ResourceManager is running
      // on.
      FileSystem fs = path.getFileSystem(conf);
      if (!fs.exists(path)) {
        throw new FileNotFoundException("File " + file + " does not exist.");
      }
      finalPath =
          path.makeQualified(fs.getUri(), fs.getWorkingDirectory()).toString();
    }
    return finalPath;
  }
}
