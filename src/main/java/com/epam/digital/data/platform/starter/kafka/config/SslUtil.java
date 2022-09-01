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

import com.epam.digital.data.platform.starter.kafka.config.properties.KafkaSslProperties;
import org.apache.kafka.clients.CommonClientConfigs;

import java.util.Collections;
import java.util.Map;

import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_KEY_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_TYPE_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG;

class SslUtil {

  private SslUtil() {}

  static Map<String, Object> createSslConfigs(KafkaSslProperties sslProperties) {
    if (!sslProperties.isEnabled()) {
      return Collections.emptyMap();
    }
    return Map.of(
        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
        KafkaSslProperties.SECURITY_PROTOCOL,
        SSL_TRUSTSTORE_TYPE_CONFIG,
        sslProperties.getTruststoreType(),
        SSL_KEYSTORE_TYPE_CONFIG,
        sslProperties.getKeystoreType(),
        SSL_TRUSTSTORE_CERTIFICATES_CONFIG,
        sslProperties.getTruststoreCertificate(),
        SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG,
        sslProperties.getKeystoreCertificate(),
        SSL_KEYSTORE_KEY_CONFIG,
        sslProperties.getKeystoreKey());
  }
}
