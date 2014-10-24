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

package org.apache.hadoop.yarn.server.api;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.protocolrecords.NotifySCMRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NotifySCMResponse;

/**
 * <p>
 * The protocol between a <code>NodeManager's</code>
 * <code>SharedCacheUploadService</code> and the
 * <code>SharedCacheManager.</code>
 * </p>
 */
@Public
@Stable
public interface NMCacheUploaderSCMProtocol {
  /**
   * <p>
   * The method used by the NodeManager's <code>SharedCacheUploadService</code>
   * to notify the shared cache manager of a newly cached resource.
   * </p>
   *
   * <p>
   * The <code>SharedCacheManager</code> responds with whether or not the
   * NodeManager should delete the uploaded file.
   * </p>
   *
   * @param request notify the shared cache manager of a newly uploaded resource
   *          to the shared cache
   * @return response indicating if the newly uploaded resource should be
   *         deleted
   * @throws YarnException
   * @throws IOException
   */
  public NotifySCMResponse notify(NotifySCMRequest request)
      throws YarnException, IOException;

}
