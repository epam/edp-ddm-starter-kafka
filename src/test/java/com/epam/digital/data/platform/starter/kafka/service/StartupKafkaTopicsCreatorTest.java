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
import com.epam.digital.data.platform.starter.kafka.config.properties.KafkaConsumer.ErrorHandler;
import com.epam.digital.data.platform.starter.kafka.config.properties.KafkaProperties.TopicProperties;
import com.epam.digital.data.platform.starter.kafka.exception.CreateKafkaTopicException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicExistsException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.kafka.common.config.TopicConfig.RETENTION_MS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StartupKafkaTopicsCreatorTest {

  private static final String REQUEST_REPLY_ROOT_NAME = "test";
  private static final String PLAIN_TOPIC = "plain-topic";
  private static final Set<String> EXISTING_TOPICS = Set.of(REQUEST_REPLY_ROOT_NAME + "-inbound", "some-topic");

  private static final int RETENTION_DAYS_FOR_PREFIX_MATCHING_TOPIC = 2;
  private static final String RETENTION_MS_FOR_PREFIX_MATCHING_TOPIC = Long.toString(TimeUnit.DAYS.toMillis(RETENTION_DAYS_FOR_PREFIX_MATCHING_TOPIC));

  private static final int DEFAULT_RETENTION_DAYS = 365;
  private static final String DEFAULT_RETENTION_MS = Long.toString(TimeUnit.DAYS.toMillis(DEFAULT_RETENTION_DAYS));

  private KafkaProperties kafkaProperties;
  private StartupKafkaTopicsCreator startupKafkaTopicsCreator;

  @Mock
  private AdminClient adminClient;
  @Mock
  private KafkaFuture<Void> createTopicsFuture;
  @Mock
  private KafkaFuture<Set<String>> listTopicsFuture;
  @Mock
  private CreateTopicsResult createTopicsResult;
  @Mock
  private ListTopicsResult listTopicsResult;
  @Captor
  private ArgumentCaptor<Set<NewTopic>> argumentCaptor;

  @BeforeEach
  void setup() {
    kafkaProperties = createKafkaProperties();
    startupKafkaTopicsCreator = new StartupKafkaTopicsCreator(() -> adminClient, kafkaProperties);
  }

  @Test
  void shouldCreateAllMissingTopics() throws Exception {
    customizeAdminClientMock();

    startupKafkaTopicsCreator.createKafkaTopics();

    verify(adminClient).createTopics(argumentCaptor.capture());

    Set<String> topicsForCreation = getTopicsForCreation(argumentCaptor);
    Set<String> missingTopics = getMissingTopics();

    assertEquals(missingTopics, topicsForCreation);
  }

  @Test
  void shouldThrowExceptionWhenCannotConnectToKafka() {
    when(adminClient.listTopics()).thenThrow(new CreateKafkaTopicException("any", null));

    assertThrows(CreateKafkaTopicException.class,
        () -> startupKafkaTopicsCreator.createKafkaTopics());
  }

  @Test
  void shouldNotFailOnTopicExistsException() throws Exception {
    doReturn(EXISTING_TOPICS).when(listTopicsFuture).get(anyLong(), any(TimeUnit.class));
    when(listTopicsResult.names()).thenReturn(listTopicsFuture);
    when(adminClient.listTopics()).thenReturn(listTopicsResult);
    when(createTopicsResult.values()).thenReturn(Collections.singletonMap("some-topic", createTopicsFuture));
    when(createTopicsFuture.get(anyLong(), any(TimeUnit.class)))
        .thenThrow(new RuntimeException(new TopicExistsException("")));
    when(adminClient.createTopics(anyCollection())).thenReturn(createTopicsResult);

    startupKafkaTopicsCreator.createKafkaTopics();

    verify(adminClient).createTopics(argumentCaptor.capture());
  }

  @Test
  void expectCorrectRetentionPolicyCreated() throws Exception {
    // given
    kafkaProperties = createKafkaProperties();
    startupKafkaTopicsCreator = new StartupKafkaTopicsCreator(() -> adminClient, kafkaProperties);
    customizeAdminClientMock();

    // when
    startupKafkaTopicsCreator.createKafkaTopics();

    // then
    verify(adminClient).createTopics(argumentCaptor.capture());
    Set<NewTopic> createdTopics = argumentCaptor.getValue();

    createdTopics.forEach(topic -> {
      if (topic.name().startsWith(REQUEST_REPLY_ROOT_NAME)) {
        assertEquals(RETENTION_MS_FOR_PREFIX_MATCHING_TOPIC, topic.configs().get(RETENTION_MS_CONFIG));
      } else {
        assertEquals(DEFAULT_RETENTION_MS, topic.configs().get(RETENTION_MS_CONFIG));
      }
    });
  }

  private Set<String> getTopicsForCreation(ArgumentCaptor<Set<NewTopic>> setArgumentCaptor) {
    return setArgumentCaptor.getValue()
        .stream()
        .map(NewTopic::name)
        .collect(Collectors.toSet());
  }

  private Set<String> getMissingTopics() {
    Set<String> requiredTopics = new HashSet<>();
    requiredTopics.add(REQUEST_REPLY_ROOT_NAME + "-inbound");
    requiredTopics.add(REQUEST_REPLY_ROOT_NAME + "-outbound");
    requiredTopics.add(REQUEST_REPLY_ROOT_NAME + "-inbound.DLT");
    requiredTopics.add(PLAIN_TOPIC);
    requiredTopics.add(PLAIN_TOPIC + ".DLT");
    requiredTopics.removeAll(EXISTING_TOPICS);
    return requiredTopics;
  }

  private void customizeAdminClientMock() throws Exception {
    doReturn(EXISTING_TOPICS).when(listTopicsFuture).get(anyLong(), any(TimeUnit.class));
    when(listTopicsResult.names()).thenReturn(listTopicsFuture);
    when(adminClient.listTopics()).thenReturn(listTopicsResult);
    when(createTopicsResult.values()).thenReturn(Map.of("some-topic", createTopicsFuture));
    when(adminClient.createTopics(anyCollection())).thenReturn(createTopicsResult);
  }

  private KafkaProperties createKafkaProperties() {
    var handler = new KafkaProperties.RequestReplyHandler();
    handler.setRequest(REQUEST_REPLY_ROOT_NAME + "-inbound");
    handler.setReply(REQUEST_REPLY_ROOT_NAME + "-outbound");

    var requestReplyTopics = Map.of(REQUEST_REPLY_ROOT_NAME, handler);
    var plainTopics = Map.of("topic-root", PLAIN_TOPIC);

    KafkaProperties kafkaProperties = new KafkaProperties();
    kafkaProperties.getConsumer().setErrorHandler(new ErrorHandler());
    kafkaProperties.getConsumer().getErrorHandler().setMaxElapsedTime(5000L);
    kafkaProperties.setTopicProperties(new TopicProperties());
    kafkaProperties.getRequestReply().setTopics(requestReplyTopics);
    kafkaProperties.setTopics(plainTopics);

    var retention = new KafkaProperties.TopicsRetentionPolicy();

    var retentionPrefixPolicy = new KafkaProperties.TopicsGroupRetentionPolicy();
    retentionPrefixPolicy.setTopicPrefixes(Collections.singletonList(REQUEST_REPLY_ROOT_NAME));
    retentionPrefixPolicy.setDays(RETENTION_DAYS_FOR_PREFIX_MATCHING_TOPIC);
    retention.setPolicies(Collections.singletonList(retentionPrefixPolicy));
    retention.setDefaultInDays(DEFAULT_RETENTION_DAYS);

    kafkaProperties.getTopicProperties().setRetention(retention);

    var topicCreationConfig = new KafkaProperties.TopicCreationProperties();
    topicCreationConfig.setEnabled(true);
    topicCreationConfig.setEnabledDlq(true);
    topicCreationConfig.setNumPartitions(1);
    topicCreationConfig.setReplicationFactor((short) 1);
    topicCreationConfig.setTimeoutInSeconds(60L);
    kafkaProperties.getTopicProperties().setCreation(topicCreationConfig);

    return kafkaProperties;
  }
}
