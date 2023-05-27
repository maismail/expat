/**
 * This file is part of Expat
 * Copyright (C) 2022, Hopsworks AB. All rights reserved
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

package io.hops.hopsworks.expat.migrations.serving;

import io.hops.hopsworks.common.hdfs.DistributedFileSystemOps;
import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import io.hops.hopsworks.expat.db.DbConnectionFactory;
import io.hops.hopsworks.expat.migrations.MigrateStep;
import io.hops.hopsworks.expat.migrations.MigrationException;
import io.hops.hopsworks.expat.migrations.RollbackException;
import io.hops.hopsworks.expat.migrations.projects.util.HopsClient;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.hadoop.fs.Path;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ServingModelFrameworkMigration implements MigrateStep {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServingModelFrameworkMigration.class);
  
  protected Connection connection;
  private boolean dryRun;
  private String hopsUser;
  
  private final static String GET_SERVINGS = "SELECT id, model_path, model_version FROM serving";
  private final static String UPDATE_SERVING = "UPDATE serving SET model_framework = ? WHERE id = ?";
  
  private final static String MODEL_SUMMARY_XATTR_NAMESPACE_NAME = "provenance.model_summary";
  
  public enum ModelFramework {
    //Note: since we map enum directly to the DB the order is important!
    TENSORFLOW,
    PYTHON,
    SKLEARN,
    TORCH
  }
  
  @Override
  public void migrate() throws MigrationException {
    LOGGER.info("Starting serving model framework migration");
    
    try {
      setup();
    } catch (SQLException | ConfigurationException ex) {
      String errorMsg = "Could not initialize database connection";
      LOGGER.error(errorMsg);
      throw new MigrationException(errorMsg, ex);
    }
    
    PreparedStatement getServingsStmt = null;
    PreparedStatement updateServingStmt = null;
    DistributedFileSystemOps dfso = null;
    try {
      connection.setAutoCommit(false);
      dfso = HopsClient.getDFSO(hopsUser);
      
      // -- per serving
      updateServingStmt = connection.prepareStatement(UPDATE_SERVING);
      getServingsStmt = connection.prepareStatement(GET_SERVINGS);
      ResultSet servingsResultSet = getServingsStmt.executeQuery();
      while (servingsResultSet.next()) {
        // parse query result
        int servingId = servingsResultSet.getInt(1);
        String modelPath = servingsResultSet.getString(2);
        Integer modelVersion = servingsResultSet.getInt(3);
        
        // get model framework attr:
        // NOTE: PYTHON is considered as default value
        ModelFramework modelFramework = getModelFramework(dfso, modelPath, modelVersion);
        
        // add serving update to batch
        updateServingStmt.setInt(1, modelFramework.ordinal()); // model_framework
        updateServingStmt.setInt(2, servingId);
        updateServingStmt.addBatch();
      }
      
      // update servings
      if (dryRun) {
        LOGGER.info(updateServingStmt.toString());
      } else {
        updateServingStmt.executeBatch();
      }
      
      connection.commit();
      connection.setAutoCommit(true);
    } catch (IllegalStateException | SQLException ex) {
      String errorMsg = "Could not migrate serving model framework";
      LOGGER.error(errorMsg);
      throw new MigrationException(errorMsg, ex);
    } finally {
      closeConnections(updateServingStmt, getServingsStmt);
      if (dfso != null) {
        dfso.close();
      }
    }
    LOGGER.info("Finished serving model framework migration");
  }
  
  @Override
  public void rollback() throws RollbackException {
    // noop
  }
  
  private ModelFramework getModelFramework(DistributedFileSystemOps dfso, String modelPath, Integer modelVersion) {
    String modelVersionPath = modelPath + "/" + modelVersion;
    byte[] modelSummaryBytes = null;
    try {
      modelSummaryBytes = dfso.getXAttr(new Path(modelVersionPath), MODEL_SUMMARY_XATTR_NAMESPACE_NAME);
    } catch (IOException ex) {
      LOGGER.info("Model framework XAttr not found in model version directory '%s', using default value instead",
        modelVersionPath);
    }
    if (modelSummaryBytes != null) {
      JSONObject modelSummary = new JSONObject(new String(modelSummaryBytes));
      if (modelSummary.has("framework")) {
        String framework = modelSummary.getString("framework");
        try {
          return ModelFramework.valueOf(framework);
        } catch (IllegalArgumentException ex) {
          LOGGER.info("Unknown model framework '%s', using default value instead", framework);
        }
      }
    }
    // if the model version does not exist or does not contain framework, set PYTHON by default
    return ModelFramework.PYTHON;
  }
  
  private void setup() throws SQLException, ConfigurationException {
    Configuration conf = ConfigurationBuilder.getConfiguration();
    dryRun = conf.getBoolean(ExpatConf.DRY_RUN);
    hopsUser = conf.getString(ExpatConf.HOPS_CLIENT_USER);
    if (hopsUser == null) {
      throw new ConfigurationException(ExpatConf.HOPS_CLIENT_USER + " cannot be null");
    }
    connection = DbConnectionFactory.getConnection();
  }
  
  private void closeConnections(PreparedStatement... stmts) {
    try {
      for (PreparedStatement stmt : stmts) {
        if (stmt != null) {
          stmt.close();
        }
      }
    } catch (SQLException ex) {
      //do nothing
    }
  }
}
