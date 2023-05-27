/**
 * This file is part of Expat
 * Copyright (C) 2020, Logical Clocks AB. All rights reserved
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
package io.hops.hopsworks.expat.migrations.projects.appprovenance;

import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import io.hops.hopsworks.expat.db.DbConnectionFactory;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.sql.Connection;
import java.sql.SQLException;

public class ReindexV1 implements MigrateStep {
  private final static Logger LOGGER = LoggerFactory.getLogger(ReindexV1.class);
  
  protected Connection connection;
  private CloseableHttpClient httpClient;
  private HttpHost elastic;
  private String elasticUser;
  private String elasticPass;
  /** index rename - https://github.com/logicalclocks/hopsworks-ee/pull/285/files#diff
  -06c751df9fcf17dfb751c43138c3a56fa5a757a51bede65a9d181d3d64967090L2056 */
  private String oldIndex = "app_prov";
  private String newIndex = "app_provenance";
  
  private void setup()
    throws SQLException, ConfigurationException, GeneralSecurityException {
    connection = DbConnectionFactory.getConnection();
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
      .setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(CookieSpecs.IGNORE_COOKIES).build())
      .setSSLContext(new SSLContextBuilder().loadTrustMaterial((x509Certificates, s) -> true).build())
      .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
      .build();
  }
  
  private void close() throws SQLException, IOException {
    if(connection != null) {
      connection.close();
    }
    if(httpClient != null) {
      httpClient.close();
    }
  }
  
  @Override
  public void migrate() throws MigrationException {
    try {
      setup();
      LOGGER.info("migrate - reindexing");
      if(!ElasticClient.indexExists(httpClient, elastic, elasticUser, elasticPass, oldIndex)) {
        LOGGER.info("migrate - missing <old> index:" + oldIndex);
        return;
      }
      if(!ElasticClient.indexExists(httpClient, elastic, elasticUser, elasticPass, newIndex)) {
        LOGGER.info("migrate - missing <new> index:" + newIndex);
        return;
      }
      int initNewCount = ElasticClient.itemCount(httpClient, elastic, elasticUser, elasticPass, newIndex);
      ElasticClient.reindex(httpClient, elastic, elasticUser, elasticPass, oldIndex, newIndex);
      int oldCount = ElasticClient.itemCount(httpClient, elastic, elasticUser, elasticPass, oldIndex);
      int reindexNewCount = ElasticClient.itemCount(httpClient, elastic, elasticUser, elasticPass, newIndex);
      if(oldCount != reindexNewCount) {
        LOGGER.info("migrate - reindexed, mismatch item count old index:{}, new index:{},{}",
          oldCount, initNewCount, reindexNewCount);
      }
      LOGGER.info("migrate - reindexed");
    } catch (SQLException | ConfigurationException | GeneralSecurityException | IOException | URISyntaxException e) {
      throw new MigrationException("error", e);
    } finally {
      try {
        close();
      } catch (SQLException | IOException e) {
        throw new MigrationException("error on close", e);
      }
    }
  }
  
  @Override
  public void rollback() throws RollbackException {
    try {
      setup();
      LOGGER.info("rollback - reindexing");
      if(!ElasticClient.indexExists(httpClient, elastic, elasticUser, elasticPass, oldIndex)) {
        LOGGER.info("rollback - missing <old> index:" + oldIndex);
        return;
      }
      if(!ElasticClient.indexExists(httpClient, elastic, elasticUser, elasticPass, newIndex)) {
        LOGGER.info("rollback - missing <new> index:" + newIndex);
        return;
      }
      int initOldCount = ElasticClient.itemCount(httpClient, elastic, elasticUser, elasticPass, oldIndex);
      ElasticClient.reindex(httpClient, elastic, elasticUser, elasticPass, newIndex, oldIndex);
      int newCount = ElasticClient.itemCount(httpClient, elastic, elasticUser, elasticPass, newIndex);
      int reindexOldCount = ElasticClient.itemCount(httpClient, elastic, elasticUser, elasticPass, oldIndex);
      if(newCount != reindexOldCount) {
        LOGGER.info("rollback - reindexed, mismatch item count new index:{}, old index:{},{}",
          newCount, initOldCount, reindexOldCount);
      }
      
      LOGGER.info("rollback - reindexed");
    } catch (SQLException | ConfigurationException | GeneralSecurityException | IOException | URISyntaxException e) {
      throw new RollbackException("error", e);
    } finally {
      try {
        close();
      } catch (SQLException | IOException e) {
        throw new RollbackException("error on close", e);
      }
    }
  }
}
