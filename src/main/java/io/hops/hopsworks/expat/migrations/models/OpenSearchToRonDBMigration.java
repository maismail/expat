package io.hops.hopsworks.expat.migrations.models;
/**
 * This file is part of Expat
 * Copyright (C) 2023, Logical Clocks AB. All rights reserved
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

import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import io.hops.hopsworks.expat.db.DbConnectionFactory;
import io.hops.hopsworks.expat.db.dao.hdfs.inode.ExpatHdfsInode;
import io.hops.hopsworks.expat.db.dao.hdfs.inode.ExpatInodeController;
import io.hops.hopsworks.expat.db.dao.hdfs.user.ExpatHdfsUser;
import io.hops.hopsworks.expat.db.dao.hdfs.user.ExpatHdfsUserFacade;
import io.hops.hopsworks.expat.db.dao.models.ExpatModel;
import io.hops.hopsworks.expat.db.dao.models.ExpatModelVersion;
import io.hops.hopsworks.expat.db.dao.models.ExpatModelsController;
import io.hops.hopsworks.expat.db.dao.project.ExpatProject;
import io.hops.hopsworks.expat.db.dao.project.ExpatProjectFacade;
import io.hops.hopsworks.expat.db.dao.user.ExpatUser;
import io.hops.hopsworks.expat.db.dao.user.ExpatUserFacade;
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
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;

public class OpenSearchToRonDBMigration implements MigrateStep {
  private final static Logger LOGGER = LoggerFactory.getLogger(OpenSearchToRonDBMigration.class);

  protected ExpatModelsController expatModelsController;
  protected ExpatInodeController expatInodeController;
  protected ExpatProjectFacade expatProjectFacade;
  protected ExpatHdfsUserFacade expatHdfsUserFacade;
  protected ExpatUserFacade expatUserFacade;

  protected boolean dryRun;

  protected Connection connection;
  private CloseableHttpClient httpClient;
  private HttpHost elastic;
  private String elasticUser;
  private String elasticPass;

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
    this.expatModelsController = new ExpatModelsController(this.connection);
    this.expatInodeController = new ExpatInodeController(this.connection);
    this.expatProjectFacade = new ExpatProjectFacade(ExpatProject.class, this.connection);
    this.expatUserFacade = new ExpatUserFacade();
    this.expatHdfsUserFacade = new ExpatHdfsUserFacade(ExpatHdfsUser.class, this.connection);
    this.dryRun = conf.getBoolean(ExpatConf.DRY_RUN);
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
      LOGGER.info("Getting all file provenance indices");
      JSONObject fileProvIndices = ElasticClient.getIndicesByRegex(httpClient, elastic, elasticUser, elasticPass,
        "*__file_prov");
      if (fileProvIndices.length() == 10000) {
        throw new MigrationException("Unexpected large amount of file provenance indices detected. This migration " +
                "does not handle more than 10000 file provenance indices. " +
                "Migration needs to be fixed to handle pagination");
      }
      if (fileProvIndices.length() > 0) {
        LOGGER.info("Found {} file provenance indices to migrate", fileProvIndices.keySet().size());
        for(String fileProvIndexName: fileProvIndices.keySet()) {
          long projectInodeId = Long.parseLong(fileProvIndexName.substring(0, fileProvIndexName.indexOf("__")));
          ExpatHdfsInode projectInode = expatInodeController.getInodeById(projectInodeId);
          if (projectInode == null) {
            LOGGER.warn("Project inode does not exist " + projectInodeId + ", skipping migration");
          } else {
            String projectName = projectInode.getName();
            ExpatHdfsInode modelDatasetInode = expatInodeController.getInodeAtPath(
                    String.format("/Projects/%s/Models", projectName));
            if (modelDatasetInode == null) {
              LOGGER.info("Project " + projectName + " does NOT have a Models dataset. Continue...");
              continue;
            }
            String query = "{\"track_total_hits\":true,\"from\":0,\"size\":10000,\"query\":{\"bool\":" +
                    "{\"must\":[{\"term\":{\"entry_type\":" +
                    "{\"value\":\"state\",\"boost\":1.0}}},{\"bool\":{\"should\":[{\"term\":{\"project_i_id\":" +
                    "{\"value\":\"" + projectInode.getId() + "\",\"boost\":1.0}}}]" +
                    ",\"adjust_pure_negative\":true,\"boost\":1.0}},{\"bool\":" +
                    "{\"should\":[{\"term\":{\"ml_type\":{\"value\":\"MODEL\",\"boost\":1.0}}}]," +
                    "\"adjust_pure_negative\":true,\"boost\":1.0}},{\"bool\":{\"should\":[{\"term\":{\"dataset_i_id\":"
                    +
                    "{\"value\":\"" + modelDatasetInode.getId() + "\",\"boost\":1.0}}}]" +
                    ",\"adjust_pure_negative\":true,\"boost\":1.0}},{\"exists\":" +
                    "{\"field\":\"xattr_prov.model_summary.value\",\"boost\":1.0}}]," +
                    "\"adjust_pure_negative\":true,\"boost\":1.0}}}";

            JSONObject resp = ElasticClient.search(httpClient, elastic, elasticUser, elasticPass, fileProvIndexName,
                    query);

            JSONObject modelSearchHits = resp.getJSONObject("hits");
            int totalHits = modelSearchHits.getJSONObject("total").getInt("value");
            JSONArray modelHits = modelSearchHits.getJSONArray("hits");
            if(totalHits > modelHits.length()) {
              throw new MigrationException("There are more documents in opensearch than what will be consumed by" +
                      " the migration, totalHits=" + totalHits + ", modelHits=" + modelHits.length());
            }
            if (modelHits.length() > 0) {
              LOGGER.info("Migrating {} model versions for project {}", modelHits.length(), projectInode.getName());
              for (int y = 0; y < modelHits.length(); y++) {
                JSONObject modelHit = modelHits.getJSONObject(y);
                JSONObject source = modelHit.getJSONObject("_source");
                JSONObject xattrProv = source.getJSONObject("xattr_prov");
                JSONObject modelSummary = xattrProv.getJSONObject("model_summary");
                JSONObject value = modelSummary.getJSONObject("value");

                ExpatProject expatProject = expatProjectFacade.findByProjectName(projectInode.getName());
                Integer userId = getModelVersionCreator(expatProject, source);

                String modelName = null;
                if (value.has("name")) {
                  modelName = value.getString("name");
                } else {
                  throw new MigrationException("name field missing from model: " + source.toString(4));
                }

                Integer version = null;
                if (value.has("version")) {
                  version = value.getInt("version");
                } else {
                  throw new MigrationException("version field missing from model: " + source.toString(4));
                }

                Long created = new Date().getTime();
                if (source.has("create_timestamp")) {
                  created = source.getLong("create_timestamp");
                }

                String description = null;
                if (value.has("description")) {
                  description = value.getString("description");
                }

                String metrics = null;
                if (value.has("metrics")) {
                  Object metricsObj;
                  metricsObj = value.get("metrics");
                  if (metricsObj instanceof JSONObject) {
                    JSONObject migratedMetrics = new JSONObject();
                    migratedMetrics.put("attributes", metricsObj);
                    metrics = migratedMetrics.toString();
                  }
                }

                String program = null;
                if (value.has("program")) {
                  program = value.getString("program");
                }

                String framework = "PYTHON";
                if (value.has("framework")) {
                  framework = value.getString("framework");
                }

                String environment = null;
                if (projectName != null && modelName != null && version != null) {
                  environment = String.format("/Projects/%s/Models/%s/%s/environment.yml",
                          projectName, modelName, version);
                }

                String experimentId = null;
                if (value.has("experimentId")) {
                  experimentId = value.getString("experimentId");
                }

                String experimentProjectName = null;
                if (value.has("experimentProjectName")) {
                  experimentProjectName = value.getString("experimentProjectName");
                }

                ExpatModel expatModel = expatModelsController.getByProjectAndName(expatProject.getId(), modelName);
                if (expatModel == null) {
                  LOGGER.info("Could not find model {} for project {}, creating it", modelName, expatProject.getName());
                  expatModel = expatModelsController.insertModel(connection, modelName, expatProject.getId(),
                          false);
                }
                ExpatModelVersion expatModelVersion = expatModelsController.insertModelVersion(connection,
                        expatModel.getId(), version, userId, created, description, metrics, program,
                        framework, environment, experimentId, experimentProjectName, dryRun);
                LOGGER.info("Successfully migrated model {} version {} for project {}", expatModel.getName(), version,
                        expatProject.getName());
              }
            } else {
              LOGGER.info("Found no model versions to migrate for project {}", projectInode.getName());
            }
          }
        }
      }
    } catch (SQLException | ConfigurationException | GeneralSecurityException | IOException | URISyntaxException |
             IllegalAccessException | InstantiationException e) {
      throw new MigrationException("error", e);
    } finally {
      try {
        close();
      } catch (SQLException | IOException e) {
        throw new MigrationException("error on close", e);
      }
    }
  }

  private Integer getModelVersionCreator(ExpatProject project, JSONObject source)
    throws SQLException, IllegalAccessException, InstantiationException {
    if(!source.has("user_id")) {
      return getProjectCreator(project);
    }
    ExpatHdfsUser expatHdfsUser = expatHdfsUserFacade.find(source.getInt("user_id"));
    if(expatHdfsUser == null) {
      return getProjectCreator(project);
    }
    String hopsworksUsername = expatHdfsUser.getName().split("__")[1];
    ExpatUser user = expatUserFacade.getExpatUserByUsername(connection, hopsworksUsername);
    if(user == null) {
      return getProjectCreator(project);
    }
    return user.getUid();
  }

  private Integer getProjectCreator(ExpatProject project) throws SQLException {
    LOGGER.info("Fallback to project creator for model version " + project.getOwner());
    return expatUserFacade.getExpatUserByEmail(connection, project.getOwner()).getUid();
  }

  @Override
  public void rollback() throws RollbackException {
    //Empty model/model_version table
  }
}
