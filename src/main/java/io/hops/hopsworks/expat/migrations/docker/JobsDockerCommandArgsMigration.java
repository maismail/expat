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
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;


public class JobsDockerCommandArgsMigration implements MigrateStep {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobsDockerCommandArgsMigration.class);
  private final static String GET_ALL_DOCKER_CONFIGURATIONS =
          "SELECT id, json_config FROM jobs WHERE type = ?";
  private final static String UPDATE_SPECIFIC_JOB_JSON_CONFIG = "UPDATE jobs SET json_config = ? WHERE id = ?";
  protected Connection connection;

  private void setup() throws SQLException, ConfigurationException {
    connection = DbConnectionFactory.getConnection();
  }

  @Override
  public void migrate() throws MigrationException {
    LOGGER.info("Starting docker jobs command args migration");
    try {
      setup();
    } catch (SQLException | ConfigurationException ex) {
      String errorMsg = "Could not initialize database connection";
      LOGGER.error(errorMsg);
      throw new MigrationException(errorMsg, ex);
    }

    PreparedStatement getAllDockerJobsStmt = null;
    PreparedStatement updateJSONConfigStmt = null;
    try {
      connection.setAutoCommit(false);
      getAllDockerJobsStmt = connection.prepareStatement(GET_ALL_DOCKER_CONFIGURATIONS);
      getAllDockerJobsStmt.setString(1, "DOCKER");
      ResultSet jobsResultSet = getAllDockerJobsStmt.executeQuery();

      updateJSONConfigStmt = connection.prepareStatement(UPDATE_SPECIFIC_JOB_JSON_CONFIG);
      while (jobsResultSet.next()) {
        String args;
        String command;
        int id = jobsResultSet.getInt(1);
        String oldConfig = jobsResultSet.getString(2);
        if(oldConfig != null) {

          LOGGER.info("Trying to migrate JobID: " + id);
          JSONObject config = new JSONObject(oldConfig);
          LOGGER.info("config:" + config);
          if (config.has("args")) {
            args = config.getString("args");
            config.remove("args");
            config.put("defaultArgs", args);
          }
          if (config.has("command")) {
            command = config.getString("command");
            config.remove("command");
            JSONArray commandList = new JSONArray();
            commandList.put(command);
            config.put("command", commandList);
          }

          String newConfig = config.toString();
          LOGGER.info("newConfig:" + newConfig);
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
      closeConnections(getAllDockerJobsStmt, updateJSONConfigStmt);
    }
    LOGGER.info("Finished jobConfig migration");
  }

  @Override
  public void rollback() throws RollbackException {
    LOGGER.info("Starting docker jobs command args rollback.");
    try {
      setup();
    } catch (SQLException | ConfigurationException ex) {
      String errorMsg = "Could not initialize database connection";
      LOGGER.error(errorMsg);
      throw new RollbackException(errorMsg, ex);
    }

    PreparedStatement getAllDockerJobsStmt = null;
    PreparedStatement updateJSONConfigStmt = null;
    try {
      connection.setAutoCommit(false);
      getAllDockerJobsStmt = connection.prepareStatement(GET_ALL_DOCKER_CONFIGURATIONS);
      getAllDockerJobsStmt.setString(1, "DOCKER");
      ResultSet jobsResultSet = getAllDockerJobsStmt.executeQuery();

      updateJSONConfigStmt = connection.prepareStatement(UPDATE_SPECIFIC_JOB_JSON_CONFIG);
      while (jobsResultSet.next()) {
        String defaultArgs;
        int id = jobsResultSet.getInt(1);
        String oldConfig = jobsResultSet.getString(2);
        if(oldConfig != null) {

          LOGGER.info("Trying to rollback JobID: " + id);
          JSONObject config = new JSONObject(oldConfig);

          if (config.has("defaultArgs")) {
            defaultArgs = config.getString("defaultArgs");
            config.remove("defaultArgs");
            config.put("args", defaultArgs);
          }
          if (config.has("command")) {
            JSONArray command = config.getJSONArray("command");
            if (command != null && command.length() > 0) {
              config.put("command", command.getString(0));
            }
          }
          
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
      closeConnections(getAllDockerJobsStmt, updateJSONConfigStmt);
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
