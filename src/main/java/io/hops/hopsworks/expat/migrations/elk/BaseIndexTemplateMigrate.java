/**
 * This file is part of Expat
 * Copyright (C) 2021, Logical Clocks AB. All rights reserved
 *
 * Expat is free software: you can redistribute it and/or modify it under the terms of
 * the GNU Affero General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 *
 * Expat is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with
 * this program. If not, see <https://www.gnu.org/licenses/>.
 *
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

public class BaseIndexTemplateMigrate implements MigrateStep  {
  private static final Logger LOGGER = LogManager.getLogger(BaseIndexTemplateMigrate.class);
  
  private HttpHost elastic;
  private String elasticUser;
  private String elasticPass;
  private CloseableHttpClient httpClient;
  
  private static final String PROJECTS_INDEX = "projects";
  private static final String PROJECTS_MAPPING =
    "\"mappings\":{"
      + "\"dynamic\":\"strict\","
      + "\"properties\":{"
      + "\"doc_type\":{ \"type\":\"keyword\" },"
      + "\"project_id\":{\"type\":\"integer\" },"
      + "\"dataset_id\":{\"type\":\"long\" },"
      + "\"public_ds\":{\"type\":\"boolean\" },"
      + "\"description\":{\"type\":\"text\" },"
      + "\"name\":{\"type\":\"text\" },"
      + "\"parent_id\":{\"type\":\"long\" },"
      + "\"partition_id\":{ \"type\":\"long\" },"
      + "\"user\":{ \"type\":\"keyword\" },"
      + "\"group\":{ \"type\":\"keyword\" },"
      + "\"operation\":{ \"type\":\"short\" },"
      + "\"size\":{ \"type\":\"long\" },"
      + "\"timestamp\":{ \"type\":\"long\" },"
      + "\"xattr\":{ \"type\":\"nested\", \"dynamic\":true }"
      + "}}";
  
  private static final String FEATURESTORE_INDEX = "featurestore";
  private static final String FEATURESTORE_MAPPING =
    "\"mappings\":{"
      + "\"dynamic\":\"strict\","
      + "\"properties\":{"
      + "\"doc_type\":{\"type\":\"keyword\"},"
      + "\"name\":{\"type\":\"text\"},"
      + "\"version\":{\"type\":\"integer\"},"
      + "\"project_id\":{\"type\":\"integer\"},"
      + "\"project_name\":{\"type\":\"text\"},"
      + "\"dataset_iid\":{\"type\":\"long\"},"
      + "\"xattr\":{\"type\":\"nested\",\"dynamic\":true}"
      + "}}";
  
  private static final String APP_PROVENANCE_INDEX = "app_provenance";
  private static final String APP_PROVENANCE_MAPPING =
    "\"mappings\":{"
      + "\"properties\":{"
      + "\"app_id\":{\"type\":\"keyword\"},"
      + "\"app_state\":{\"type\":\"keyword\"},"
      + "\"timestamp\":{\"type\":\"long\"},"
      + "\"app_name\":{\"type\":\"text\"},"
      + "\"app_user\":{\"type\":\"text\"}"
      + "}}";

  private void setup()
    throws ConfigurationException, KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
    
    Configuration conf = ConfigurationBuilder.getConfiguration();
    
    String elasticURI = conf.getString(ExpatConf.ELASTIC_URI);
    if (elasticURI == null) {
      throw new ConfigurationException(ExpatConf.ELASTIC_URI + " cannot be null");
    }
    elastic = HttpHost.create(elasticURI);
    
    elasticUser = conf.getString(ExpatConf.ELASTIC_USER_KEY);
    if (elasticUser == null) {
      throw new ConfigurationException(ExpatConf.ELASTIC_USER_KEY + " cannot be null");
    }
    elasticPass = conf.getString(ExpatConf.ELASTIC_PASS_KEY);
    if (elasticPass == null) {
      throw new ConfigurationException(ExpatConf.ELASTIC_PASS_KEY + " cannot be null");
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
      ElasticClient.createTemplate(httpClient, elastic, elasticUser, elasticPass,
        PROJECTS_INDEX, PROJECTS_MAPPING, PROJECTS_INDEX);
      ElasticClient.createTemplate(httpClient, elastic, elasticUser, elasticPass,
        FEATURESTORE_INDEX, FEATURESTORE_MAPPING, FEATURESTORE_INDEX);
      ElasticClient.createTemplate(httpClient, elastic, elasticUser, elasticPass,
        APP_PROVENANCE_INDEX, APP_PROVENANCE_MAPPING, APP_PROVENANCE_INDEX);
    } catch (ConfigurationException | KeyStoreException | NoSuchAlgorithmException | KeyManagementException |
      IOException e) {
      throw new MigrationException("elastic error", e);
    }
  }
  
  @Override
  public void rollback() throws RollbackException {
    try {
      LOGGER.info("setup");
      setup();
      LOGGER.info("templates");
      ElasticClient.deleteTemplate(httpClient, elastic, elasticUser, elasticPass, PROJECTS_INDEX);
      ElasticClient.deleteTemplate(httpClient, elastic, elasticUser, elasticPass, FEATURESTORE_INDEX);
      ElasticClient.deleteTemplate(httpClient, elastic, elasticUser, elasticPass, APP_PROVENANCE_INDEX);
    } catch (ConfigurationException | KeyStoreException | NoSuchAlgorithmException | KeyManagementException |
      IOException e) {
      throw new RollbackException("elastic error", e);
    }
  }
}
