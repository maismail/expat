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

package io.hops.hopsworks.expat.migrations.docker;

import io.hops.hopsworks.expat.db.DbConnectionFactory;
import io.hops.hopsworks.expat.migrations.MigrateStep;
import io.hops.hopsworks.expat.migrations.MigrationException;
import io.hops.hopsworks.expat.migrations.RollbackException;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class JobsDockerResourcesMigration implements MigrateStep {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobsDockerResourcesMigration.class);
  private final static String GET_ALL_DOCKER_AND_PYTHON_CONFIGURATIONS =
    "SELECT id, json_config FROM jobs WHERE type = ? OR type = ?";
  private final static String UPDATE_SPECIFIC_JOB_JSON_CONFIG = "UPDATE jobs SET json_config = ? WHERE id = ?";
  protected Connection connection;
  private final int defaultMemory = 1024;
  private final int defaultCores = 1;
  private final int defaultGPUs = 0;

  private void setup() throws SQLException, ConfigurationException {
    connection = DbConnectionFactory.getConnection();
  }

  @Override
  public void migrate() throws MigrationException {
    LOGGER.info("Starting docker and python docker resources migration");
    try {
      setup();
    } catch (SQLException | ConfigurationException ex) {
      String errorMsg = "Could not initialize database connection";
      LOGGER.error(errorMsg);
      throw new MigrationException(errorMsg, ex);
    }

    PreparedStatement getAllDockerAndPythonJobsStmt = null;
    PreparedStatement updateJSONConfigStmt = null;
    try {
      connection.setAutoCommit(false);
      getAllDockerAndPythonJobsStmt = connection.prepareStatement(GET_ALL_DOCKER_AND_PYTHON_CONFIGURATIONS);
      getAllDockerAndPythonJobsStmt.setString(1, "DOCKER");
      getAllDockerAndPythonJobsStmt.setString(2, "PYTHON");
      ResultSet jobsResultSet = getAllDockerAndPythonJobsStmt.executeQuery();

      updateJSONConfigStmt = connection.prepareStatement(UPDATE_SPECIFIC_JOB_JSON_CONFIG);
      while (jobsResultSet.next()) {
        Integer memory;
        Integer cores;
        Integer gpus;
        int id = jobsResultSet.getInt(1);
        String oldConfig = jobsResultSet.getString(2);
        if(oldConfig != null) {

          LOGGER.info("Trying to migrate JobID: " + id);
          JSONObject config = new JSONObject(oldConfig);
          if (config.has("memory")) {
            memory = config.getInt("memory");
            config.remove("memory");
          } else {
            memory = defaultMemory;
          }
          if (config.has("cores")) {
            cores = config.getInt("cores");
            config.remove("cores");
          } else {
            cores = defaultCores;
          }
          if (config.has("gpus")) {
            gpus = config.getInt("gpus");
            config.remove("gpus");
          } else {
            gpus = defaultGPUs;
          }

          JSONObject dockerResourcesConfig = new JSONObject();
          dockerResourcesConfig.put("type", "dockerResourcesConfiguration");
          dockerResourcesConfig.put("memory", memory);
          dockerResourcesConfig.put("cores", cores);
          dockerResourcesConfig.put("gpus", gpus);

          config.put("resourceConfig", dockerResourcesConfig);

          String newConfig = config.toString();
          LOGGER.info("Successfully migrated JobID: " + id);

          updateJSONConfigStmt.setString(1, newConfig);
          updateJSONConfigStmt.setInt(2, id);
          updateJSONConfigStmt.addBatch();
        }
      }
      updateJSONConfigStmt.executeBatch();
      connection.commit();
      connection.setAutoCommit(true);
    } catch(SQLException ex) {
      String errorMsg = "Could not migrate job configurations";
      LOGGER.error(errorMsg);
      throw new MigrationException(errorMsg, ex);
    } finally {
      closeConnections(getAllDockerAndPythonJobsStmt, updateJSONConfigStmt);
    }
    LOGGER.info("Finished jobConfig migration");
  }

  @Override
  public void rollback() throws RollbackException {
    LOGGER.info("Starting docker and python docker resources rollback");
    try {
      setup();
    } catch (SQLException | ConfigurationException ex) {
      String errorMsg = "Could not initialize database connection";
      LOGGER.error(errorMsg);
      throw new RollbackException(errorMsg, ex);
    }

    PreparedStatement getAllDockerAndPythonJobsStmt = null;
    PreparedStatement updateJSONConfigStmt = null;
    try {
      connection.setAutoCommit(false);
      getAllDockerAndPythonJobsStmt = connection.prepareStatement(GET_ALL_DOCKER_AND_PYTHON_CONFIGURATIONS);
      getAllDockerAndPythonJobsStmt.setString(1, "DOCKER");
      getAllDockerAndPythonJobsStmt.setString(2, "PYTHON");
      ResultSet jobsResultSet = getAllDockerAndPythonJobsStmt.executeQuery();

      updateJSONConfigStmt = connection.prepareStatement(UPDATE_SPECIFIC_JOB_JSON_CONFIG);
      while (jobsResultSet.next()) {
        Integer memory;
        Integer cores;
        Integer gpus;
        int id = jobsResultSet.getInt(1);
        String oldConfig = jobsResultSet.getString(2);
        if(oldConfig != null) {

          LOGGER.info("Trying to rollback JobID: " + id);
          JSONObject config = new JSONObject(oldConfig);
          JSONObject dockerResource = config.getJSONObject("resourceConfig");

          if (dockerResource.has("memory")) {
            memory = dockerResource.getInt("memory");
          } else {
            memory = defaultMemory;
          }
          if (dockerResource.has("cores")) {
            cores = dockerResource.getInt("cores");
          } else {
            cores = defaultCores;
          }
          if (dockerResource.has("gpus")) {
            gpus = dockerResource.getInt("gpus");
          } else {
            gpus = defaultGPUs;
          }
          config.remove("resourceConfig");

          config.put("memory", memory);
          config.put("cores", cores);
          config.put("gpus", gpus);

          String newConfig = config.toString();
          LOGGER.info("Successfully rollbacked JobID: " + id);

          updateJSONConfigStmt.setString(1, newConfig);
          updateJSONConfigStmt.setInt(2, id);
          updateJSONConfigStmt.addBatch();
        }
      }
      updateJSONConfigStmt.executeBatch();
      connection.commit();
      connection.setAutoCommit(true);
    } catch(SQLException ex) {
      String errorMsg = "Could not rollback job configurations";
      LOGGER.error(errorMsg);
      throw new RollbackException(errorMsg, ex);
    } finally {
      closeConnections(getAllDockerAndPythonJobsStmt, updateJSONConfigStmt);
    }
    LOGGER.info("Finished jobConfig rollback");
  }

  private void closeConnections(PreparedStatement stmt1, PreparedStatement stmt2) {
    try {
      if(stmt1 != null) {
        stmt1.close();
      }
      if(stmt2 != null) {
        stmt2.close();
      }
    } catch(SQLException ex) {
      //do nothing
    }
  }
}
