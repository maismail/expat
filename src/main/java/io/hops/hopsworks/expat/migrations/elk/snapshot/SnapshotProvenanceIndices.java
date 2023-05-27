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
package io.hops.hopsworks.expat.migrations.elk.snapshot;

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
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;

public class SnapshotProvenanceIndices implements MigrateStep {
  private final static Logger LOGGER = LoggerFactory.getLogger(SnapshotProvenanceIndices.class);
  
  protected Connection connection = null;
  boolean dryrun = false;
  
  protected HttpHost elastic;
  protected String elasticUser;
  protected String elasticPass;
  protected CloseableHttpClient httpClient;
  
  protected String snapshotRepoName = null;
  protected String snapshotName = null;
  protected Boolean ignoreUnavailable = null;
  
  private final static String GET_ALL_PROJECTS = "SELECT i.name, i.id FROM hops.hdfs_inodes i JOIN hopsworks.project " +
    "p ON p.inode_pid=i.parent_id AND p.partition_id=i.partition_id AND p.inode_name=i.name";
  private final static int GET_ALL_PROJECTS_S_NAME = 1;
  private final static int GET_ALL_PROJECTS_S_IID = 2;
  
  protected void setup()
    throws ConfigurationException, KeyStoreException, NoSuchAlgorithmException, KeyManagementException, SQLException {
    
    Configuration conf = ConfigurationBuilder.getConfiguration();
  
    connection = DbConnectionFactory.getConnection();
  
    dryrun = conf.getBoolean(ExpatConf.DRY_RUN);
    
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
    
    snapshotRepoName = conf.getString(ExpatConf.ELASTIC_SNAPSHOT_REPO_NAME);
    if (snapshotRepoName == null) {
      throw new ConfigurationException(ExpatConf.ELASTIC_SNAPSHOT_REPO_NAME + " cannot be null");
    }
    snapshotName = conf.getString(ExpatConf.ELASTIC_SNAPSHOT_NAME);
    if (snapshotName == null) {
      throw new ConfigurationException(ExpatConf.ELASTIC_SNAPSHOT_NAME + " cannot be null");
    }
    ignoreUnavailable = conf.getBoolean(ExpatConf.ELASTIC_SNAPSHOT_IGNORE_UNAVAILABLE);
    
    httpClient = HttpClients
      .custom()
      .setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(
        CookieSpecs.IGNORE_COOKIES).build())
      .setSSLContext(new SSLContextBuilder().loadTrustMaterial((x509Certificates, s) -> true).build())
      .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
      .build();
  }
  
  protected void close() throws IOException {
    if(httpClient != null) {
      httpClient.close();
    }
  }
  
  @Override
  public void migrate() throws MigrationException {
    Throwable tAux = null;
    try {
      setup();
      traverseElements(dryrun);
    } catch (Throwable t) {
      tAux = t;
      throw new MigrationException("error", t);
    } finally {
      try {
        close();
      } catch (IOException e) {
        if(tAux != null) {
          throw new MigrationException("error", e);
        } else {
          LOGGER.warn("could not close http client:{}", e.getStackTrace().toString());
        }
      }
    }
  }
  
  @Override
  public void rollback() throws RollbackException {
    Throwable tAux = null;
    try {
      setup();
      traverseElements(dryrun);
    } catch (Throwable t) {
      tAux = t;
      throw new RollbackException("error", t);
    } finally {
      try {
        close();
      } catch (IOException e) {
        if(tAux != null) {
          throw new RollbackException("error", e);
        } else {
          LOGGER.warn("could not close http client:{}", e.getStackTrace().toString());
        }
      }
    }
  }
  
  private void traverseElements(boolean dryRun)
    throws Exception {
    connection.setAutoCommit(false);
  
    try (PreparedStatement allProjectsStmt = connection.prepareStatement(GET_ALL_PROJECTS)) {
      ResultSet allProjectsResult = allProjectsStmt.executeQuery();
    
      LinkedList<String> bulkIndices = new LinkedList<>();
      while (allProjectsResult.next()) {
        String project = allProjectsResult.getString(GET_ALL_PROJECTS_S_NAME);
        long projectInodeId = allProjectsResult.getLong(GET_ALL_PROJECTS_S_IID);
        String provIndex = projectInodeId + "__file_prov";
        LOGGER.info("snapshot: {} of project:{}", provIndex, project);
        bulkIndices.add(provIndex);
      }
      if(!bulkIndices.isEmpty() && !dryRun){
        ElasticClient.takeSnapshot(httpClient, elastic, elasticUser, elasticPass,
          snapshotRepoName, snapshotName, ignoreUnavailable, bulkIndices.toArray(new String[bulkIndices.size()]));
      }
      connection.commit();
    }
    connection.setAutoCommit(true);
  }
}
