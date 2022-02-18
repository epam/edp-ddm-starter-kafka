/*
 * Copyright 2021 EPAM Systems.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.epam.digital.data.platform.starter.kafka.config.properties;

import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaConsumer {
  private boolean enabled;
  private Class<?> keyDeserializer = StringDeserializer.class;
  private Class<?> valueDeserializer = StringDeserializer.class;
  private List<String> trustedPackages = new ArrayList<>();
  private String groupId;
  private ErrorHandler errorHandler = new ErrorHandler();
  private Map<String, Object> customConfig = new HashMap<>();

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public Class<?> getKeyDeserializer() {
    return keyDeserializer;
  }

  public void setKeyDeserializer(Class<?> keyDeserializer) {
    this.keyDeserializer = keyDeserializer;
  }

  public Class<?> getValueDeserializer() {
    return valueDeserializer;
  }

  public void setValueDeserializer(Class<?> valueDeserializer) {
    this.valueDeserializer = valueDeserializer;
  }

  public List<String> getTrustedPackages() {
    return trustedPackages;
  }

  public void setTrustedPackages(List<String> trustedPackages) {
    this.trustedPackages = trustedPackages;
  }

  public String getGroupId() {
    return groupId;
  }

  public void setGroupId(String groupId) {
    this.groupId = groupId;
  }

  public ErrorHandler getErrorHandler() {
    return errorHandler;
  }

  public void setErrorHandler(ErrorHandler errorHandler) {
    this.errorHandler = errorHandler;
  }

  public Map<String, Object> getCustomConfig() {
    return customConfig;
  }

  public void setCustomConfig(Map<String, Object> customConfig) {
    this.customConfig = customConfig;
  }

  public static class ErrorHandler {

    private boolean enabledDlq;
    private Long initialInterval = 1500L;
    private Long maxElapsedTime = 6000L;
    private Double multiplier = 2D;

    public boolean isEnabledDlq() {
      return enabledDlq;
    }

    public void setEnabledDlq(boolean enabledDlq) {
      this.enabledDlq = enabledDlq;
    }

    public Long getInitialInterval() {
      return initialInterval;
    }

    public void setInitialInterval(Long initialInterval) {
      this.initialInterval = initialInterval;
    }

    public Long getMaxElapsedTime() {
      return maxElapsedTime;
    }

    public void setMaxElapsedTime(Long maxElapsedTime) {
      this.maxElapsedTime = maxElapsedTime;
    }

    public Double getMultiplier() {
      return multiplier;
    }

    public void setMultiplier(Double multiplier) {
      this.multiplier = multiplier;
    }
  }
}
