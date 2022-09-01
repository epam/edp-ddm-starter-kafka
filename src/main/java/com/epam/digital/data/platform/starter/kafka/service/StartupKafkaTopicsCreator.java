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

package com.epam.digital.data.platform.starter.kafka.service;

import com.epam.digital.data.platform.starter.kafka.config.properties.KafkaProperties;
import com.epam.digital.data.platform.starter.kafka.exception.CreateKafkaTopicException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.common.config.TopicConfig.RETENTION_MS_CONFIG;

@ConditionalOnProperty(
    prefix = "data-platform.kafka.topic-properties.creation",
    name = "enabled",
    havingValue = "true")
@Component
public class StartupKafkaTopicsCreator {

  private static final String DLQ_TOPIC_SUFFIX = ".DLT";

  private final Logger log = LoggerFactory.getLogger(StartupKafkaTopicsCreator.class);

  private final Supplier<AdminClient> adminClientFactory;
  private final KafkaProperties kafkaProperties;

  public StartupKafkaTopicsCreator(
      Supplier<AdminClient> adminClientFactory, KafkaProperties kafkaProperties) {
    this.adminClientFactory = adminClientFactory;
    this.kafkaProperties = kafkaProperties;
  }

  @PostConstruct
  public void createKafkaTopics() {
    try (var kafkaAdminClient = adminClientFactory.get()) {
      var missingTopicNames = getMissingTopicNames(kafkaAdminClient);
      createTopics(missingTopicNames, kafkaAdminClient);
    }
  }

  private Set<String> getMissingTopicNames(AdminClient kafkaAdminClient) {
    Set<String> existingTopics;
    try {
      existingTopics =
          kafkaAdminClient
              .listTopics()
              .names()
              .get(kafkaProperties.getTopicProperties().getCreation().getTimeoutInSeconds(), TimeUnit.SECONDS);
    } catch (Exception e) {
      throw new CreateKafkaTopicException(
          String.format(
              "Failed to retrieve existing kafka topics in %d sec",
                  kafkaProperties.getTopicProperties().getCreation().getTimeoutInSeconds()),
          e);
    }
    Set<String> requiredTopics = getRequiredTopics();
    requiredTopics.removeAll(existingTopics);
    return requiredTopics;
  }

  private Set<String> getRequiredTopics() {
    var requestReplyTopics =
        kafkaProperties.getRequestReply().getTopics().values().stream()
            .flatMap(handler -> Stream.of(handler.getRequest(), handler.getReply()))
            .collect(Collectors.toSet());
    var simpleTopics = kafkaProperties.getTopics().values();
    return Stream.of(requestReplyTopics, simpleTopics, getDlqTopics())
        .flatMap(Collection::stream)
        .collect(Collectors.toSet());
  }

  private Set<String> getDlqTopics() {
    if (!kafkaProperties.getTopicProperties().getCreation().isEnabledDlq()) {
      return Collections.emptySet();
    }
    var requestReplyDlq =
        kafkaProperties.getRequestReply().getTopics().values().stream()
            .map(handler -> handler.getRequest() + DLQ_TOPIC_SUFFIX)
            .collect(Collectors.toSet());
    var simpleTopicsDlq =
        kafkaProperties.getTopics().values().stream()
            .map(name -> name + DLQ_TOPIC_SUFFIX)
            .collect(Collectors.toSet());
    return Stream.of(requestReplyDlq, simpleTopicsDlq)
        .flatMap(Collection::stream)
        .collect(Collectors.toSet());
  }

  private void createTopics(Set<String> topicNames, AdminClient kafkaAdminClient) {
    var createTopicsResult = kafkaAdminClient.createTopics(getNewTopics(topicNames));
    createTopicsResult.values().forEach(this::handleTopicCreationResult);
  }

  private void handleTopicCreationResult(String topicName, KafkaFuture<Void> future) {
    try {
      future.get(
          kafkaProperties.getTopicProperties().getCreation().getTimeoutInSeconds(),
          TimeUnit.SECONDS);
    } catch (Exception e) {
      if (e.getCause() instanceof TopicExistsException) {
        log.warn("Topic {} was in missing topics list, but now exists", topicName);
      } else {
        throw new CreateKafkaTopicException(
            String.format(
                "Failed to create topic %s in %d sec",
                topicName,
                kafkaProperties.getTopicProperties().getCreation().getTimeoutInSeconds()),
            e);
      }
    }
  }

  private Collection<NewTopic> getNewTopics(Set<String> requiredTopics) {
    return requiredTopics.stream()
        .map(
            topicName ->
                new NewTopic(
                    topicName,
                    kafkaProperties.getTopicProperties().getCreation().getNumPartitions(),
                    kafkaProperties.getTopicProperties().getCreation().getReplicationFactor()))
        .map(topic -> topic.configs(getRetentionPolicy(topic.name())))
        .collect(Collectors.toSet());
  }

  private Map<String, String> getRetentionPolicy(String topicName) {
    Predicate<KafkaProperties.TopicsGroupRetentionPolicy> retentionPolicyPrefixesContains =
        policy -> policy.getTopicPrefixes().stream().anyMatch(topicName::startsWith);
    var retentionPolicyInDays =
        kafkaProperties.getTopicProperties().getRetention().getPolicies().stream()
            .filter(retentionPolicyPrefixesContains)
            .findFirst()
            .map(KafkaProperties.TopicsGroupRetentionPolicy::getDays)
            .orElse(kafkaProperties.getTopicProperties().getRetention().getDefaultInDays());

    return Map.of(
        RETENTION_MS_CONFIG, Long.toString(TimeUnit.DAYS.toMillis(retentionPolicyInDays)));
  }
}
