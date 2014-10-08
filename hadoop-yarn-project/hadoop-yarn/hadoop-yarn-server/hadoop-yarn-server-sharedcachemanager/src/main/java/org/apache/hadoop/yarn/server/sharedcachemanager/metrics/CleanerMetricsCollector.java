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

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.impl.MsInfo;

/**
 * Very simple metric collector for the cleaner service
 */
@Private
@Evolving
public class CleanerMetricsCollector implements MetricsCollector {

  /**
   * A map from reporting period to the list of reported metrics in that period
   */
  Map<String, Number> metrics = new HashMap<String, Number>();
  String sessionId = null;

  @Override
  public MetricsRecordBuilder addRecord(String name) {
    // For the cleaner service we need to record only the metrics destined for the
    // cleaner service. We hence ignore the others.
    if (CleanerMetrics.isCleanerMetricRecord(name)) {
      return new CleanerMetricsRecordBuilder(metrics);
    }
    else
      return new NullMetricsRecordBuilder();
  }

  @Override
  public MetricsRecordBuilder addRecord(MetricsInfo info) {
    return addRecord(info.name());
  }

  public Map<String, Number> getMetrics() {
    return metrics;
  }
  
  public String getSessionId() {
    return sessionId;
  }

  /**
   * A builder which ignores all the added metrics, tags, etc.
   */
  class NullMetricsRecordBuilder extends MetricsRecordBuilder {

    @Override
    public MetricsRecordBuilder tag(MetricsInfo info, String value) {
      return this;
    }

    @Override
    public MetricsRecordBuilder add(MetricsTag tag) {
      return this;
    }

    @Override
    public MetricsRecordBuilder add(AbstractMetric metric) {
      return this;
    }

    @Override
    public MetricsRecordBuilder setContext(String value) {
      return this;
    }

    @Override
    public MetricsRecordBuilder addCounter(MetricsInfo info, int value) {
      return this;
    }

    @Override
    public MetricsRecordBuilder addCounter(MetricsInfo info, long value) {
      return this;
    }

    @Override
    public MetricsRecordBuilder addGauge(MetricsInfo info, int value) {
      return this;
    }

    @Override
    public MetricsRecordBuilder addGauge(MetricsInfo info, long value) {
      return this;
    }

    @Override
    public MetricsRecordBuilder addGauge(MetricsInfo info, float value) {
      return this;
    }

    @Override
    public MetricsRecordBuilder addGauge(MetricsInfo info, double value) {
      return this;
    }

    @Override
    public MetricsCollector parent() {
      return CleanerMetricsCollector.this;
    }
  }

  /**
   * A builder which keeps track of only counters and gauges
   */
  @Private
  @Evolving
  class CleanerMetricsRecordBuilder extends NullMetricsRecordBuilder {
    Map<String, Number> metricValueMap;

    public CleanerMetricsRecordBuilder(Map<String, Number> metricValueMap) {
      this.metricValueMap = metricValueMap;
    }

    @Override
    public MetricsRecordBuilder tag(MetricsInfo info, String value) {
      if (MsInfo.SessionId.equals(info))
        setSessionId(value);
      return this;
    }
    
    @Override
    public MetricsRecordBuilder add(MetricsTag tag) {
      if (MsInfo.SessionId.equals(tag.info()))
        setSessionId(tag.value());
      return this;
    }

    private void setSessionId(String value) {
      sessionId = value;
    }

    @Override
    public MetricsRecordBuilder add(AbstractMetric metric) {
      metricValueMap.put(metric.name(), metric.value());
      return this;
    }

    @Override
    public MetricsRecordBuilder addCounter(MetricsInfo info, int value) {
      metricValueMap.put(info.name(), value);
      return this;
    }

    @Override
    public MetricsRecordBuilder addCounter(MetricsInfo info, long value) {
      metricValueMap.put(info.name(),value);
      return this;
    }

    @Override
    public MetricsRecordBuilder addGauge(MetricsInfo info, int value) {
      metricValueMap.put(info.name(),value);
      return this;
    }

    @Override
    public MetricsRecordBuilder addGauge(MetricsInfo info, long value) {
      metricValueMap.put(info.name(),value);
      return this;
    }

    @Override
    public MetricsRecordBuilder addGauge(MetricsInfo info, float value) {
      metricValueMap.put(info.name(),value);
      return this;
    }

    @Override
    public MetricsRecordBuilder addGauge(MetricsInfo info, double value) {
      metricValueMap.put(info.name(),value);
      return this;
    }

    @Override
    public MetricsCollector parent() {
      return CleanerMetricsCollector.this;
    }

  }

}
