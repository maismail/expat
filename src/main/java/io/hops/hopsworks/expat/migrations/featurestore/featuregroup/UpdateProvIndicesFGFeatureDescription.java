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
package io.hops.hopsworks.expat.migrations.featurestore.featuregroup;

import com.lambdista.util.Try;
import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import io.hops.hopsworks.expat.db.DbConnectionFactory;
import io.hops.hopsworks.expat.elastic.ElasticClient;
import io.hops.hopsworks.expat.epipe.EpipeRunner;
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
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Function;

public class UpdateProvIndicesFGFeatureDescription implements MigrateStep {
  private final static Logger LOGGER = LoggerFactory.getLogger(UpdateProvIndicesFGFeatureDescription.class);
  
  /************** ELASTIC PROVENANCE ************/
  private final static String GET_ALL_PROJECTS = "SELECT partition_id, inode_pid, inode_name " +
    "FROM hopsworks.project";
  private final static int GET_ALL_PROJECTS_S_PARTITION_ID = 1;
  private final static int GET_ALL_PROJECTS_S_INODE_PID = 2;
  private final static int GET_ALL_PROJECTS_S_INODE_NAME = 3;
  private final static String GET_INODE =  "SELECT id FROM hops.hdfs_inodes " +
    "WHERE partition_id=? && parent_id=? && name=?";
  private final static int GET_INODE_S_ID = 1;
  private final static int GET_INODE_W_PARTITION_ID = 1;
  private final static int GET_INODE_W_PARENT_ID = 2;
  private final static int GET_INODE_W_NAME = 3;
  String migrateScript = "if(ctx._source.ml_type == \"FEATURE\"){ " +
    "if((ctx._source.entry_type == \"operation\" && (" +
    "     ctx._source.inode_operation == \"XATTR_ADD\" || " +
    "     ctx._source.inode_operation == \"XATTR_UPDATE\" || " +
    "     ctx._source.inode_operation == \"XATTR_DELETE\")) " +
    "   || ctx._source.entry_type == \"state\") {" +
    "  if(ctx._source.containsKey(\"xattr_prov\") && ctx._source.xattr_prov.containsKey(\"featurestore\")) {" +
    "    def fg_features = new ArrayList();" +
    "    for (fg in ctx._source.xattr_prov.featurestore.value.fg_features) {" +
    "      fg_features.add([\"name\":fg]);" +
    "    }" +
    "    ctx._source.xattr_prov.featurestore.value.remove(\"fg_features\");" +
    "    ctx._source.xattr_prov.featurestore.value.fg_features = fg_features;" +
    "  }" +
    "}" +
    "}";
  String rollbackScript = "if(ctx._source.ml_type == \"FEATURE\"){ " +
    "if((ctx._source.entry_type == \"operation\" && (" +
    "     ctx._source.inode_operation == \"XATTR_ADD\" || " +
    "     ctx._source.inode_operation == \"XATTR_UPDATE\" || " +
    "     ctx._source.inode_operation == \"XATTR_DELETE\")) " +
    "   || ctx._source.entry_type == \"state\") {" +
    "  if(ctx._source.containsKey(\"xattr_prov\") && ctx._source.xattr_prov.containsKey(\"featurestore\")) {" +
    "    def fg_features = new ArrayList();" +
    "    for (fg in ctx._source.xattr_prov.featurestore.value.fg_features) {" +
    "      fg_features.add(fg.name);" +
    "    }" +
    "    ctx._source.xattr_prov.featurestore.value.remove(\"fg_features\");" +
    "    ctx._source.xattr_prov.featurestore.value.fg_features = fg_features;" +
    "  }" +
    "}" +
    "}";
  /**********************************************/
  boolean dryrun = false;
  
  protected Connection connection = null;
  private HttpHost elastic;
  private String elasticUser;
  private String elasticPass;
  private CloseableHttpClient httpClient;
  
  private void setup()
    throws ConfigurationException, KeyStoreException, NoSuchAlgorithmException, SQLException, KeyManagementException {
    Configuration conf = ConfigurationBuilder.getConfiguration();
  
    dryrun = conf.getBoolean(ExpatConf.DRY_RUN);
  
    connection = DbConnectionFactory.getConnection();
    
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
  
  private void close() throws SQLException {
    if(connection != null) {
      connection.close();
    }
  }
  
  @Override
  public void migrate() throws MigrationException {
    LOGGER.info("provenance indices - description re-mapping");
    try {
      setup();
    
      if(dryrun) {
        traverseProvIndices((indexName) -> {
          LOGGER.info("migrate prov index:{}", indexName);
          return new Try.Success<>("done");
        });
      } else {
        EpipeRunner.waitForEpipeIdle(connection);
        EpipeRunner.stopEpipe();
        traverseProvIndices(processIndex("migrate", migrateScript));
        EpipeRunner.restartEpipe();
      }
    } catch (Throwable e) {
      throw new MigrationException("error", e);
    } finally {
      try {
        close();
      } catch (SQLException e) {
        throw new MigrationException("error", e);
      }
    }
  }
  
  @Override
  public void rollback() throws RollbackException {
    LOGGER.info("featuregroup feature description rollback");
    try {
      setup();
      if(dryrun) {
        traverseProvIndices((indexName) -> {
          LOGGER.info("rollback prov index:{}", indexName);
          return new Try.Success<>("done");
        });
      } else {
        EpipeRunner.stopEpipe();
        traverseProvIndices(processIndex("rollback", rollbackScript));
        EpipeRunner.restartEpipe();
      }
    } catch (Throwable e) {
      throw new RollbackException("error", e);
    } finally {
      try {
        close();
      } catch (SQLException e) {
        throw new RollbackException("error", e);
      }
    }
  }
  
  private PreparedStatement getProjectInodeStmt(ResultSet allProjectsResultSet) throws SQLException {
    PreparedStatement projectInodeStmt = connection.prepareStatement(GET_INODE);
    projectInodeStmt.setLong(GET_INODE_W_PARTITION_ID,
      allProjectsResultSet.getLong(GET_ALL_PROJECTS_S_PARTITION_ID));
    projectInodeStmt.setLong(GET_INODE_W_PARENT_ID,
      allProjectsResultSet.getLong(GET_ALL_PROJECTS_S_INODE_PID));
    projectInodeStmt.setString(GET_INODE_W_NAME,
      allProjectsResultSet.getString(GET_ALL_PROJECTS_S_INODE_NAME));
    return projectInodeStmt;
  }
  
  private void traverseProvIndices(Function<String, Try<String>> provIndexAction) throws Throwable {
    PreparedStatement allProjectsStmt = null;
    PreparedStatement projectInodeStmt = null;
    try {
      connection.setAutoCommit(false);
      //get all projects
      allProjectsStmt = connection.prepareStatement(GET_ALL_PROJECTS);
      ResultSet allProjectsResultSet = allProjectsStmt.executeQuery();
      while (allProjectsResultSet.next()) {
        //get project inode
        projectInodeStmt = getProjectInodeStmt(allProjectsResultSet);
        ResultSet projectInodeResultSet = projectInodeStmt.executeQuery();
        if(!projectInodeResultSet.next()) {
          throw new IllegalStateException("project inode not found");
        }
        Long projectInodeId = projectInodeResultSet.getLong(GET_INODE_S_ID);
        String provIndex = projectInodeId + "__file_prov";
        provIndexAction.apply(provIndex).checkedGet();
      }
    } finally {
      if (allProjectsStmt != null) {
        allProjectsStmt.close();
      }
      if(projectInodeStmt != null) {
        projectInodeStmt.close();
      }
    }
  }
  
  private Function<String, Try<String>> processIndex(String type, String script) {
    return (String indexName) -> {
      try {
        if(!ElasticClient.indexExists(httpClient, elastic, elasticUser, elasticPass, indexName)) {
          LOGGER.info("skipping project as prov index:{} does not exit", indexName);
        } else {
          LOGGER.info("{} prov index:{}", type, indexName);
          ElasticClient.reindex(httpClient, elastic, elasticUser, elasticPass, indexName, "temp_" + indexName, script);
          LOGGER.info("{} prov index:{} restructured mapping", type, indexName);
          ElasticClient.deleteIndex(httpClient, elastic, elasticUser, elasticPass, indexName);
          Thread.sleep(2000);
          ElasticClient.reindex(httpClient, elastic, elasticUser, elasticPass, "temp_" + indexName, indexName);
          ElasticClient.deleteIndex(httpClient, elastic, elasticUser, elasticPass, "temp_" + indexName);
          LOGGER.info("{} prov index:{} completed", type, indexName);
        }
        return new Try.Success<>("done");
      } catch(IOException | URISyntaxException | InterruptedException e) {
        return new Try.Failure<>(e);
      }
    };
  }
}
