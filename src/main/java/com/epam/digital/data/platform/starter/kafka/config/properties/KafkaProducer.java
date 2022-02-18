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

import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

public class KafkaProducer {
  private boolean enabled;
  private Class<?> keySerializer = StringSerializer.class;
  private Class<?> valueSerializer = JsonSerializer.class;
  private Map<String, Object> customConfig = new HashMap<>();

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public Class<?> getKeySerializer() {
    return keySerializer;
  }

  public void setKeySerializer(Class<?> keySerializer) {
    this.keySerializer = keySerializer;
  }

  public Class<?> getValueSerializer() {
    return valueSerializer;
  }

  public void setValueSerializer(Class<?> valueSerializer) {
    this.valueSerializer = valueSerializer;
  }

  public Map<String, Object> getCustomConfig() {
    return customConfig;
  }

  public void setCustomConfig(Map<String, Object> customConfig) {
    this.customConfig = customConfig;
  }
}
