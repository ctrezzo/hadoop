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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.api.NMCacheUploaderSCMProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.NotifySCMRequest;
import org.apache.hadoop.yarn.server.sharedcachemanager.metrics.NMCacheUploaderSCMProtocolMetrics;
import org.apache.hadoop.yarn.server.sharedcachemanager.store.InMemorySCMStore;
import org.apache.hadoop.yarn.server.sharedcachemanager.store.SCMStore;
import org.apache.hadoop.yarn.server.sharedcachemanager.store.SharedCacheResourceReference;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * Basic unit tests for the NodeManger to SCM Protocol Service.
 */
public class TestNMCacheUploaderSCMProtocolService {
  private static File testDir = null;

  @BeforeClass
  public static void setupTestDirs() throws IOException {
    testDir = new File("target",
        TestNMCacheUploaderSCMProtocolService.class.getCanonicalName());
    testDir.delete();
    testDir.mkdirs();
    testDir = testDir.getAbsoluteFile();
  }

  @AfterClass
  public static void cleanupTestDirs() throws IOException {
    if (testDir != null) {
      testDir.delete();
    }
  }

  private NMCacheUploaderSCMProtocolService service;
  private NMCacheUploaderSCMProtocol proxy;
  private SCMStore store;
  private final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  @Before
  public void startUp() {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.SCM_STORE_CLASS,
        InMemorySCMStore.class.getName());
    conf.set(YarnConfiguration.SHARED_CACHE_ROOT, testDir.getPath());
    AppChecker appChecker = mock(AppChecker.class);
    store = new InMemorySCMStore(appChecker);
    store.init(conf);
    store.start();

    service = new NMCacheUploaderSCMProtocolService(store);
    service.init(conf);
    service.start();

    YarnRPC rpc = YarnRPC.create(new Configuration());

    InetSocketAddress scmAddress =
        conf.getSocketAddr(YarnConfiguration.NM_SCM_ADDRESS,
            YarnConfiguration.DEFAULT_NM_SCM_ADDRESS,
            YarnConfiguration.DEFAULT_NM_SCM_PORT);

    proxy =
        (NMCacheUploaderSCMProtocol) rpc.getProxy(
            NMCacheUploaderSCMProtocol.class, scmAddress, conf);
  }

  @After
  public void cleanUp() {
    if (store != null) {
      store.stop();
    }

    if (service != null) {
      service.stop();
    }

    if (proxy != null) {
      RPC.stopProxy(proxy);
    }
  }

  @Test
  public void testNotify_noEntry() throws Exception {
    long accepted =
        NMCacheUploaderSCMProtocolMetrics.getInstance().getAcceptedUploads();

    NotifySCMRequest request =
        recordFactory.newRecordInstance(NotifySCMRequest.class);
    request.setResourceKey("key1");
    request.setFilename("foo.jar");
    assertTrue(proxy.notify(request).getAccepted());
    Collection<SharedCacheResourceReference> set =
        store.getResourceReferences("key1");
    assertNotNull(set);
    assertEquals(0, set.size());

    assertEquals(
        "NM upload metrics aren't updated.", 1,
        NMCacheUploaderSCMProtocolMetrics.getInstance().getAcceptedUploads() -
            accepted);

  }

  @Test
  public void testNotify_entryExists_differentName() throws Exception {

    long rejected =
        NMCacheUploaderSCMProtocolMetrics.getInstance().getRejectUploads();

    store.addResource("key1", "foo.jar");
    NotifySCMRequest request =
        recordFactory.newRecordInstance(NotifySCMRequest.class);
    request.setResourceKey("key1");
    request.setFilename("foobar.jar");
    assertFalse(proxy.notify(request).getAccepted());
    Collection<SharedCacheResourceReference> set =
        store.getResourceReferences("key1");
    assertNotNull(set);
    assertEquals(0, set.size());
    assertEquals(
        "NM upload metrics aren't updated.", 1,
        NMCacheUploaderSCMProtocolMetrics.getInstance().getRejectUploads() -
            rejected);

  }

  @Test
  public void testNotify_entryExists_sameName() throws Exception {

    long accepted =
        NMCacheUploaderSCMProtocolMetrics.getInstance().getAcceptedUploads();

    store.addResource("key1", "foo.jar");
    NotifySCMRequest request =
        recordFactory.newRecordInstance(NotifySCMRequest.class);
    request.setResourceKey("key1");
    request.setFilename("foo.jar");
    assertTrue(proxy.notify(request).getAccepted());
    Collection<SharedCacheResourceReference> set =
        store.getResourceReferences("key1");
    assertNotNull(set);
    assertEquals(0, set.size());
    assertEquals(
        "NM upload metrics aren't updated.", 1,
        NMCacheUploaderSCMProtocolMetrics.getInstance().getAcceptedUploads() -
            accepted);

  }
}
