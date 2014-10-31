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

package org.apache.hadoop.yarn.sharedcache;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

@Public
@Evolving
/**
 * A factory class for creating checksum objects based on a configurable
 * algorithm implementation
 */
public class SharedCacheChecksumFactory {
  private static final ConcurrentMap<String,SharedCacheChecksum> instances =
      new ConcurrentHashMap<String,SharedCacheChecksum>();

  /**
   * Get a new <code>SharedCacheChecksum</code> object based on the configurable
   * algorithm implementation
   * (see <code>yarn.sharedcache.checksum.algo.impl</code>)
   *
   * @return <code>SharedCacheChecksum</code> object
   */
  public static SharedCacheChecksum getChecksum(Configuration conf) {
    String className =
        conf.get(YarnConfiguration.SHARED_CACHE_CHECKSUM_ALGO_IMPL,
            YarnConfiguration.DEFAULT_SHARED_CACHE_CHECKSUM_ALGO_IMPL);
    SharedCacheChecksum checksum = instances.get(className);
    if (checksum == null) {
      try {
        Class<?> clazz = Class.forName(className);
        checksum = (SharedCacheChecksum) clazz.newInstance();
        SharedCacheChecksum old = instances.putIfAbsent(className, checksum);
        if (old != null) {
          checksum = old;
        }
      } catch (Exception e) {
        throw new YarnRuntimeException(e);
      }
    }

    return checksum;
  }
}
