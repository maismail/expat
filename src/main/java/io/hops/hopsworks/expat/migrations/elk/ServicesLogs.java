/**
 * This file is part of Expat
 * Copyright (C) 2021, Logical Clocks AB. All rights reserved
 * <p>
 * Expat is free software: you can redistribute it and/or modify it under the terms of
 * the GNU Affero General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 * <p>
 * Expat is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public License along with
 * this program. If not, see <https://www.gnu.org/licenses/>.
 */
package io.hops.hopsworks.expat.migrations.elk;

import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import io.hops.hopsworks.expat.elastic.ElasticClient;
import io.hops.hopsworks.expat.migrations.MigrateStep;
import io.hops.hopsworks.expat.migrations.MigrationException;
import io.hops.hopsworks.expat.migrations.RollbackException;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.http.HttpHost;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;

public class ServicesLogs implements MigrateStep {
  private static final Logger LOGGER = LogManager.getLogger(ServicesLogs.class);
  
  private HttpHost elastic;
  private HttpHost kibana;
  private String elasticUser;
  private String elasticPass;
  private String elasticServicesLogsUser;
  private String elasticServicesLogsPass;
  private CloseableHttpClient httpClient;
  
  private static final String SERVICES_INDEX = "services";
  private static final String SERVICES_INDEX_PATTERN = ".services-*";
  private static final String SERVICES_TIME_FIELD_NAME = "logdate";
  private static final String SERVICES_MAPPING =
      "\"mappings\":" +
          "{\"properties\":" +
          "{\"service\":{\"type\":\"keyword\"}," +
          "\"host\":{\"type\":\"keyword\"}," +
          "\"priority\":{\"type\":\"keyword\"}," +
          "\"logger_name\":{\"type\":\"text\"}," +
          "\"log_message\":{\"type\":\"text\"}," +
          "\"logdate\":{\"type\":\"date\"}}}";
  
  
  private void setup()
      throws ConfigurationException, KeyStoreException,
      NoSuchAlgorithmException, KeyManagementException {
    
    Configuration conf = ConfigurationBuilder.getConfiguration();
    
    String elasticURI = conf.getString(ExpatConf.ELASTIC_URI);
    if (elasticURI == null) {
      throw new ConfigurationException(
          ExpatConf.ELASTIC_URI + " cannot be null");
    }
    elastic = HttpHost.create(elasticURI);
    
    String kibanaURI = conf.getString(ExpatConf.KIBANA_URI);
    if (kibanaURI == null) {
      throw new ConfigurationException(
          ExpatConf.KIBANA_URI + " cannot be null");
    }
    kibana = HttpHost.create(kibanaURI);
    
    elasticUser = conf.getString(ExpatConf.ELASTIC_USER_KEY);
    if (elasticUser == null) {
      throw new ConfigurationException(
          ExpatConf.ELASTIC_USER_KEY + " cannot be null");
    }
    elasticPass = conf.getString(ExpatConf.ELASTIC_PASS_KEY);
    if (elasticPass == null) {
      throw new ConfigurationException(
          ExpatConf.ELASTIC_PASS_KEY + " cannot be null");
    }
  
    elasticServicesLogsUser = conf.getString(ExpatConf.ELASTIC_SERVICES_USER_KEY);
    if (elasticServicesLogsUser == null) {
      throw new ConfigurationException(
          ExpatConf.ELASTIC_SERVICES_USER_KEY + " cannot be null");
    }
  
    elasticServicesLogsPass = conf.getString(ExpatConf.ELASTIC_SERVICES_PASS_KEY);
    if (elasticServicesLogsPass == null) {
      throw new ConfigurationException(
          ExpatConf.ELASTIC_SERVICES_PASS_KEY + " cannot be null");
    }
    
    httpClient = HttpClients
        .custom()
        .setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(
            CookieSpecs.IGNORE_COOKIES).build())
        .setSSLContext(new SSLContextBuilder().loadTrustMaterial((x509Certificates, s) -> true).build())
        .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
        .build();
  }
  
  @Override
  public void migrate() throws MigrationException {
    try {
      LOGGER.info("setup");
      setup();
      LOGGER.info("templates");
      ElasticClient.createTemplate(httpClient, elastic, elasticUser,
          elasticPass,
          SERVICES_INDEX, SERVICES_MAPPING, SERVICES_INDEX_PATTERN);
      ElasticClient.createKibanaIndexPattern(SERVICES_INDEX_PATTERN,
          httpClient, kibana, elasticServicesLogsUser, elasticServicesLogsPass,
          SERVICES_TIME_FIELD_NAME);
    } catch (ConfigurationException | KeyStoreException | NoSuchAlgorithmException | KeyManagementException |
        IOException e) {
      throw new MigrationException("service logs error", e);
    }
  }
  
  @Override
  public void rollback() throws RollbackException {
    try {
      LOGGER.info("setup");
      setup();
      LOGGER.info("templates");
      ElasticClient.deleteTemplate(httpClient, elastic, elasticUser,
          elasticPass, SERVICES_INDEX);
      ElasticClient.deleteKibanaIndexPattern(SERVICES_INDEX_PATTERN, httpClient,
          kibana, elasticServicesLogsUser, elasticServicesLogsPass);
    } catch (ConfigurationException | KeyStoreException | NoSuchAlgorithmException | KeyManagementException |
        IOException e) {
      throw new RollbackException("service logs error", e);
    }
  }
}
