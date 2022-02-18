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

import com.epam.digital.data.platform.starter.kafka.config.properties.KafkaConsumer;
import com.epam.digital.data.platform.starter.kafka.config.properties.KafkaProperties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@ConditionalOnProperty(
    prefix = "data-platform.kafka.consumer",
    name = "enabled",
    havingValue = "true")
@Configuration
public class KafkaConsumerConfig {

  private final KafkaProperties kafkaProperties;

  public KafkaConsumerConfig(KafkaProperties kafkaProperties) {
    this.kafkaProperties = kafkaProperties;
  }

  @ConditionalOnBean(name = {"keyDeserializer", "valueDeserializer"})
  @Bean
  public <O> ConsumerFactory<String, O> consumerFactoryCustomDeserializers(
      Deserializer<String> keyDeserializer, Deserializer<O> valueDeserializer) {
    return new DefaultKafkaConsumerFactory<>(consumerConfigs(), keyDeserializer, valueDeserializer);
  }

  @ConditionalOnMissingBean
  @Bean
  public <O> ConsumerFactory<String, O> consumerFactory() {
    return new DefaultKafkaConsumerFactory<>(consumerConfigs());
  }

  @Bean
  public <O>
      ConcurrentKafkaListenerContainerFactory<String, O> concurrentKafkaListenerContainerFactory(
          ConsumerFactory<String, O> consumerFactory,
          Optional<KafkaTemplate<String, O>> kafkaTemplate,
          CommonErrorHandler errorHandler) {
    ConcurrentKafkaListenerContainerFactory<String, O> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory);
    factory.getContainerProperties().setMissingTopicsFatal(false);
    kafkaTemplate.ifPresent(factory::setReplyTemplate);
    factory.setCommonErrorHandler(errorHandler);
    return factory;
  }

  @ConditionalOnProperty(
      prefix = "data-platform.kafka.consumer.error-handler",
      name = "enabled-dlq",
      havingValue = "true")
  @Bean
  public CommonErrorHandler dlqDeadLetterErrorHandler(KafkaOperations<String, ?> kafkaTemplate) {
    DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);
    ExponentialBackOff backOff =
        createExponentialBackOff(kafkaProperties.getConsumer().getErrorHandler());
    return new DefaultErrorHandler(recoverer, backOff);
  }

  @ConditionalOnMissingBean
  @Bean
  public CommonErrorHandler defaultDeadLetterErrorHandler() {
    ExponentialBackOff backOff =
            createExponentialBackOff(kafkaProperties.getConsumer().getErrorHandler());
    return new DefaultErrorHandler(backOff);
  }

  private Map<String, Object> consumerConfigs() {
    KafkaConsumer consumerProperties = kafkaProperties.getConsumer();
    Map<String, Object> props = new HashMap<>();
    props.computeIfAbsent(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, v -> kafkaProperties.getBootstrap());
    props.computeIfAbsent(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, v -> consumerProperties.getKeyDeserializer());
    props.computeIfAbsent(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        v -> consumerProperties.getValueDeserializer());
    props.computeIfAbsent(
        JsonDeserializer.TRUSTED_PACKAGES,
        v -> String.join(",", consumerProperties.getTrustedPackages()));
    props.computeIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, v -> consumerProperties.getGroupId());
    props.putAll(SslUtil.createSslConfigs(kafkaProperties.getSsl()));
    props.putAll(consumerProperties.getCustomConfig());
    return props;
  }

  private ExponentialBackOff createExponentialBackOff(KafkaConsumer.ErrorHandler errorHandler) {
    ExponentialBackOff backOff = new ExponentialBackOff();
    backOff.setInitialInterval(errorHandler.getInitialInterval());
    backOff.setMaxElapsedTime(errorHandler.getMaxElapsedTime());
    backOff.setMultiplier(errorHandler.getMultiplier());
    return backOff;
  }
}
