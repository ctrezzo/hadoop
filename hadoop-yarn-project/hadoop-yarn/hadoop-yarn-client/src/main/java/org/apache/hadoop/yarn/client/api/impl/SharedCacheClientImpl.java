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

package org.apache.hadoop.yarn.client.api.impl;


import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.api.ClientSCMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.ReleaseSharedCacheResourceRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UseSharedCacheResourceRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UseSharedCacheResourceResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.SharedCacheClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.sharedcache.SharedCacheChecksum;
import org.apache.hadoop.yarn.sharedcache.SharedCacheChecksumFactory;
import org.apache.hadoop.yarn.util.Records;

/**
 * An implementation of the SharedCacheClient API.
 */
@Private
@Unstable
public class SharedCacheClientImpl extends SharedCacheClient {
  private static final Log LOG = LogFactory
      .getLog(SharedCacheClientImpl.class);

  private ClientSCMProtocol scmClient;
  private InetSocketAddress scmAddress;
  private Configuration conf;
  private SharedCacheChecksum checksum;

  // If scm isn't available, we will mark this instance
  // of SharedCacheClient unusable. This is useful when
  // the caller of SharedCacheClient needs to call the same
  // instance of SharedCacheClient multiple times; it allows
  // the caller to quickly fall back to the non-SCM approach.
  private volatile boolean scmAvailable = false;

  public SharedCacheClientImpl() {
    super(SharedCacheClientImpl.class.getName());
  }

  private static InetSocketAddress getScmAddress(Configuration conf) {
    return conf.getSocketAddr(YarnConfiguration.SCM_CLIENT_SERVER_ADDRESS,
        YarnConfiguration.DEFAULT_SCM_CLIENT_SERVER_ADDRESS,
        YarnConfiguration.DEFAULT_SCM_CLIENT_SERVER_PORT);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    if (this.scmAddress == null) {
      this.scmAddress = getScmAddress(conf);
    }
    this.conf = conf;
    this.checksum = SharedCacheChecksumFactory.getChecksum(conf);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    YarnRPC rpc = YarnRPC.create(getConfig());

    this.scmClient = (ClientSCMProtocol) rpc.getProxy(
        ClientSCMProtocol.class, this.scmAddress, getConfig());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Connecting to Shared Cache Manager at " + this.scmAddress);
    }
    scmAvailable = true;
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (this.scmClient != null) {
      RPC.stopProxy(this.scmClient);
    }
    super.serviceStop();
  }

  public boolean isScmAvailable() {
    return this.scmAvailable;
  }

  @Override
  public Path use(ApplicationId applicationId, String resourceKey) {
    Path resourcePath = null;
    UseSharedCacheResourceRequest request = Records.newRecord(
        UseSharedCacheResourceRequest.class);
    request.setAppId(applicationId);
    request.setResourceKey(resourceKey);
    try {
      UseSharedCacheResourceResponse response = this.scmClient.use(request);
      if (response != null && response.getPath() != null) {
        resourcePath = new Path(response.getPath());
      }
    } catch (Exception e) {
      // Just catching IOException isn't enough.
      // RPC call can throw ConnectionException.
      // We don't handle different exceptions separately at this point.
      LOG.warn("SCM might be down. The exception is " + e.getMessage());
      e.printStackTrace();
      scmAvailable = false;
    }
    return resourcePath;
  }

  @Override
  public void release(ApplicationId applicationId, String resourceKey) {
    if (!scmAvailable) {
      return;
    }
    ReleaseSharedCacheResourceRequest request = Records.newRecord(
        ReleaseSharedCacheResourceRequest.class);
    request.setAppId(applicationId);
    request.setResourceKey(resourceKey);
    try {
      // We do not care about the response because it is empty.
      this.scmClient.release(request);
    } catch (Exception e) {
      // Just catching IOException isn't enough.
      // RPC call can throw ConnectionException.
      LOG.warn("SCM might be down. The exception is " + e.getMessage());
      e.printStackTrace();
      scmAvailable = false;
    }
  }

  @Override
  public String getFileChecksum(Path sourceFile)
      throws IOException {
    FileSystem fs = sourceFile.getFileSystem(this.conf);
    FSDataInputStream in = null;
    try {
      in = fs.open(sourceFile);
      return this.checksum.computeChecksum(in);
    } finally {
      if (in != null) {
        in.close();
      }
    }
  }
}
