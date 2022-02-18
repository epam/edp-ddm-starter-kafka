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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties("data-platform.kafka")
public class KafkaProperties {

  private Boolean enabled;
  private String bootstrap;
  private KafkaProducer producer = new KafkaProducer();
  private KafkaConsumer consumer = new KafkaConsumer();
  private RequestReply requestReply = new RequestReply();
  private Map<String, String> topics = new HashMap<>();
  private TopicProperties topicProperties = new TopicProperties();
  private KafkaSslProperties ssl = new KafkaSslProperties();

  public String getBootstrap() {
    return bootstrap;
  }

  public void setBootstrap(String bootstrap) {
    this.bootstrap = bootstrap;
  }

  public KafkaProducer getProducer() {
    return producer;
  }

  public void setProducer(KafkaProducer producer) {
    this.producer = producer;
  }

  public KafkaConsumer getConsumer() {
    return consumer;
  }

  public void setConsumer(KafkaConsumer consumer) {
    this.consumer = consumer;
  }

  public RequestReply getRequestReply() {
    return requestReply;
  }

  public void setRequestReply(RequestReply requestReply) {
    this.requestReply = requestReply;
  }

  public Map<String, String> getTopics() {
    return topics;
  }

  public void setTopics(Map<String, String> topics) {
    this.topics = topics;
  }

  public TopicProperties getTopicProperties() {
    return topicProperties;
  }

  public void setTopicProperties(TopicProperties topicProperties) {
    this.topicProperties = topicProperties;
  }

  public KafkaSslProperties getSsl() {
    return ssl;
  }

  public void setSsl(
      KafkaSslProperties ssl) {
    this.ssl = ssl;
  }

  public Boolean getEnabled() {
    return enabled;
  }

  public void setEnabled(Boolean enabled) {
    this.enabled = enabled;
  }

  public static class RequestReply {

    private boolean enabled;
    private Map<String, RequestReplyHandler> topics = new HashMap<>();
    private Long timeoutInSeconds = 30L;

    public boolean isEnabled() {
      return enabled;
    }

    public void setEnabled(boolean enabled) {
      this.enabled = enabled;
    }

    public Map<String, RequestReplyHandler> getTopics() {
      return topics;
    }

    public void setTopics(Map<String, RequestReplyHandler> topics) {
      this.topics = topics;
    }

    public Long getTimeoutInSeconds() {
      return timeoutInSeconds;
    }

    public void setTimeoutInSeconds(Long timeoutInSeconds) {
      this.timeoutInSeconds = timeoutInSeconds;
    }
  }

  public static class RequestReplyHandler {

    private String request;
    private String reply;

    public String getRequest() {
      return request;
    }

    public void setRequest(String request) {
      this.request = request;
    }

    public String getReply() {
      return reply;
    }

    public void setReply(String reply) {
      this.reply = reply;
    }
  }

  public static class TopicProperties {

    private TopicCreationProperties creation = new TopicCreationProperties();
    private TopicsRetentionPolicy retention = new TopicsRetentionPolicy();

    public TopicCreationProperties getCreation() {
      return creation;
    }

    public void setCreation(TopicCreationProperties creation) {
      this.creation = creation;
    }

    public TopicsRetentionPolicy getRetention() {
      return retention;
    }

    public void setRetention(TopicsRetentionPolicy retention) {
      this.retention = retention;
    }
  }

  public static class TopicsRetentionPolicy {
    private List<TopicsGroupRetentionPolicy> policies = new ArrayList<>();
    private Integer defaultInDays = 7;

    public List<TopicsGroupRetentionPolicy> getPolicies() {
      return policies;
    }

    public void setPolicies(List<TopicsGroupRetentionPolicy> policies) {
      this.policies = policies;
    }

    public Integer getDefaultInDays() {
      return defaultInDays;
    }

    public void setDefaultInDays(Integer defaultInDays) {
      this.defaultInDays = defaultInDays;
    }
  }

  public static class TopicsGroupRetentionPolicy {
    private List<String> topicPrefixes = new ArrayList<>();
    private Integer days;

    public List<String> getTopicPrefixes() {
      return topicPrefixes;
    }

    public void setTopicPrefixes(List<String> topicPrefixes) {
      this.topicPrefixes = topicPrefixes;
    }

    public Integer getDays() {
      return days;
    }

    public void setDays(Integer days) {
      this.days = days;
    }
  }

  public static class TopicCreationProperties {
    private boolean enabled;
    private boolean enabledDlq;
    private Integer numPartitions;
    private Short replicationFactor;
    private Long timeoutInSeconds = 60L;

    public boolean isEnabled() {
      return enabled;
    }

    public void setEnabled(boolean enabled) {
      this.enabled = enabled;
    }

    public boolean isEnabledDlq() {
      return enabledDlq;
    }

    public void setEnabledDlq(boolean enabledDlq) {
      this.enabledDlq = enabledDlq;
    }

    public Integer getNumPartitions() {
      return numPartitions;
    }

    public void setNumPartitions(Integer numPartitions) {
      this.numPartitions = numPartitions;
    }

    public Short getReplicationFactor() {
      return replicationFactor;
    }

    public void setReplicationFactor(Short replicationFactor) {
      this.replicationFactor = replicationFactor;
    }

    public Long getTimeoutInSeconds() {
      return timeoutInSeconds;
    }

    public void setTimeoutInSeconds(Long timeoutInSeconds) {
      this.timeoutInSeconds = timeoutInSeconds;
    }
  }
}
