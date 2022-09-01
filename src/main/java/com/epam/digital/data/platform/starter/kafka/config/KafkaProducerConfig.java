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

package com.epam.digital.data.platform.starter.kafka.config;

import com.epam.digital.data.platform.starter.kafka.config.properties.KafkaProducer;
import com.epam.digital.data.platform.starter.kafka.config.properties.KafkaProperties;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@ConditionalOnProperty(
    prefix = "data-platform.kafka.producer",
    name = "enabled",
    havingValue = "true")
@Configuration
public class KafkaProducerConfig {

  private final KafkaProperties kafkaProperties;

  public KafkaProducerConfig(KafkaProperties kafkaProperties) {
    this.kafkaProperties = kafkaProperties;
  }

  @ConditionalOnBean(name = { "keySerializer", "valueSerializer" })
  @Bean
  public <I> ProducerFactory<String, I> producerFactoryCustomSerializers(
          Serializer<String> keySerializer, Serializer<I> valueSerializer) {
    return new DefaultKafkaProducerFactory<>(producerConfigs(), keySerializer, valueSerializer);
  }

  @ConditionalOnMissingBean
  @Bean
  public <I> ProducerFactory<String, I> producerFactory() {
    return new DefaultKafkaProducerFactory<>(producerConfigs());
  }

  @ConditionalOnMissingBean(name = "kafkaTemplate")
  @Bean
  public <O> KafkaTemplate<String, O> kafkaTemplate(ProducerFactory<String, O> producerFactory) {
    return new KafkaTemplate<>(producerFactory);
  }

  private Map<String, Object> producerConfigs() {
    KafkaProducer producerProperties = kafkaProperties.getProducer();
    Map<String, Object> props = new HashMap<>();
    props.computeIfAbsent(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, v -> kafkaProperties.getBootstrap());
    props.computeIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, v -> producerProperties.getKeySerializer());
    props.computeIfAbsent(
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, v -> producerProperties.getValueSerializer());
    props.putAll(SslUtil.createSslConfigs(kafkaProperties.getSsl()));
    props.putAll(producerProperties.getCustomConfig());
    return props;
  }
}
