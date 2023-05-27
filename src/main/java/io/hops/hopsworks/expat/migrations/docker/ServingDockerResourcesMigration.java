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
import java.sql.SQLException;

public class ServingDockerResourcesMigration implements MigrateStep {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServingDockerResourcesMigration.class);
  private final static String UPDATE_SERVING_DOCKER_RESOURCE_CONFIG = "UPDATE serving SET docker_resource_config = ?";
  protected Connection connection;

  private void setup() throws SQLException, ConfigurationException {
    connection = DbConnectionFactory.getConnection();
  }

  @Override
  public void migrate() throws MigrationException {
    LOGGER.info("Starting serving docker resources migration");
    try {
      setup();
    } catch (SQLException | ConfigurationException ex) {
      String errorMsg = "Could not initialize database connection";
      LOGGER.error(errorMsg);
      throw new MigrationException(errorMsg, ex);
    }
    JSONObject dockerResourceConf = new JSONObject();
    dockerResourceConf.put("type", "dockerResourcesConfiguration");
    dockerResourceConf.put("memory", 1024);
    dockerResourceConf.put("cores", 1);
    dockerResourceConf.put("gpus", 0);
    String resourceConf = dockerResourceConf.toString();

    PreparedStatement updateJSONConfigStmt = null;
    try {
      connection.setAutoCommit(false);
      updateJSONConfigStmt = connection.prepareStatement(UPDATE_SERVING_DOCKER_RESOURCE_CONFIG);
      updateJSONConfigStmt.setString(1, resourceConf);
      int rowsAffected = updateJSONConfigStmt.executeUpdate();
      connection.commit();
      connection.setAutoCommit(true);
      LOGGER.info("Update successful, " + rowsAffected + " rows affected.");
    } catch(SQLException ex) {
      String errorMsg = "Could not migrate serving configurations";
      LOGGER.error(errorMsg);
      throw new MigrationException(errorMsg, ex);
    } finally {
      closeConnections(updateJSONConfigStmt);
    }
    LOGGER.info("Finished serving migration");
  }

  @Override
  public void rollback() throws RollbackException {
    LOGGER.info("Skipping rollback as affected column docker_resource_config did not exist");
  }

  private void closeConnections(PreparedStatement stmt) {
    try {
      if(stmt != null) {
        stmt.close();
      }
    } catch(SQLException ex) {
      //do nothing
    }
  }
}
