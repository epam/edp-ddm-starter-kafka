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

import com.epam.digital.data.platform.starter.kafka.config.properties.KafkaProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;

import java.time.Duration;
import java.util.UUID;

@ConditionalOnProperty(
    prefix = "data-platform.kafka.request-reply",
    name = "enabled",
    havingValue = "true")
@Configuration
public class KafkaRequestReplyConfig {

  private final KafkaProperties kafkaProperties;

  public KafkaRequestReplyConfig(KafkaProperties kafkaProperties) {
    this.kafkaProperties = kafkaProperties;
  }

  @Bean
  public <I, O> ReplyingKafkaTemplate<String, I, O> replyingKafkaTemplate(
      ProducerFactory<String, I> producerFactory,
      ConcurrentKafkaListenerContainerFactory<String, O> concurrentKafkaListenerContainerFactory) {
    String[] outboundTopics =
        kafkaProperties.getRequestReply().getTopics().values().stream()
            .map(KafkaProperties.RequestReplyHandler::getReply)
            .toArray(String[]::new);
    ConcurrentMessageListenerContainer<String, O> replyContainer =
        concurrentKafkaListenerContainerFactory.createContainer(outboundTopics);
    replyContainer.getContainerProperties().setMissingTopicsFatal(false);
    replyContainer.getContainerProperties().setGroupId(UUID.randomUUID().toString());
    ReplyingKafkaTemplate<String, I, O> kafkaTemplate =
        new ReplyingKafkaTemplate<>(producerFactory, replyContainer);
    kafkaTemplate.setSharedReplyTopic(true);
    kafkaTemplate.setDefaultReplyTimeout(
        Duration.ofSeconds(kafkaProperties.getRequestReply().getTimeoutInSeconds()));
    return kafkaTemplate;
  }
}
