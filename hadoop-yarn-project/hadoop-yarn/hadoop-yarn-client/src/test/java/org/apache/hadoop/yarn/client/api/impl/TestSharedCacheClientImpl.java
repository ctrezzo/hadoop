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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.v2.app.job.impl.JobImpl;
import org.apache.hadoop.yarn.api.ClientSCMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.ReleaseSharedCacheResourceRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UseSharedCacheResourceRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UseSharedCacheResourceResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.UseSharedCacheResourceResponsePBImpl;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Matchers.isA;
import static org.junit.Assert.assertEquals;

public class TestSharedCacheClientImpl {

  private static final Log LOG = LogFactory
      .getLog(TestSharedCacheClientImpl.class);

  public static SharedCacheClientImpl client;
  public static ClientSCMProtocol cProtocol;
  private static Path TEST_ROOT_DIR;
  private static FileSystem localFs;

  @BeforeClass
  public static void beforeClass() throws IOException {
    localFs = FileSystem.getLocal(new Configuration());
    TEST_ROOT_DIR =
        new Path("target", TestSharedCacheClientImpl.class.getName()
            + "-tmpDir").makeQualified(localFs.getUri(),
            localFs.getWorkingDirectory());
  }

  @AfterClass
  public static void afterClass() {
    try {
      if (localFs != null) {
        localFs.close();
      }
    } catch (IOException ioe) {
      LOG.info("IO exception in closing file system)");
      ioe.printStackTrace();
    }
  }

  @Before
  public void setup() {
    cProtocol = mock(ClientSCMProtocol.class);
    client = new SharedCacheClientImpl() {
      @Override
      protected ClientSCMProtocol createClientProxy() {
        return cProtocol;
      }

      @Override
      protected void stopClientProxy() {
        // do nothing because it is mocked
      }
    };
    client.init(new Configuration());
    client.start();
  }

  @After
  public void cleanup() {
    if (client != null) {
      client.stop();
      client = null;
    }
  }

  @Test
  public void testUse() throws Exception {
    Path file = new Path("viewfs://test/path");
    UseSharedCacheResourceResponse response =
        new UseSharedCacheResourceResponsePBImpl();
    response.setPath(file.toString());
    when(cProtocol.use(isA(UseSharedCacheResourceRequest.class))).thenReturn(
        response);
    Path newPath = client.use(mock(ApplicationId.class), "key");
    assertEquals(file, newPath);
  }

  @Test
  public void testRelease() throws Exception {
    // Release does not care about the return value because it is empty
    when(cProtocol.release(isA(ReleaseSharedCacheResourceRequest.class)))
        .thenReturn(null);
    client.release(mock(ApplicationId.class), "key");
  }

  @Test
  public void testChecksum() throws Exception {

  }
}
