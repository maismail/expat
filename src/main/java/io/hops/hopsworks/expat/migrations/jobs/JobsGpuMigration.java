package io.hops.hopsworks.expat.migrations.jobs;

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
import java.sql.Statement;

public class JobsGpuMigration implements MigrateStep {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobsGpuMigration.class);
  private final static String GET_ALL_JOB_CONFIGURATIONS = "SELECT id, json_config FROM jobs";
  private final static String UPDATE_SPECIFIC_JOB_JSON_CONFIG = "UPDATE jobs SET json_config = ? WHERE id = ?";
  protected Connection connection;

  private void setup() throws SQLException, ConfigurationException {
    connection = DbConnectionFactory.getConnection();
  }

  @Override
  public void migrate() throws MigrationException {
    LOGGER.info("Starting jobConfig migration");
    try {
      setup();
    } catch (SQLException | ConfigurationException ex) {
      String errorMsg = "Could not initialize database connection";
      LOGGER.error(errorMsg);
      throw new MigrationException(errorMsg, ex);
    }

    Statement stmt = null;
    PreparedStatement updateJSONConfigStmt = null;
    try {
      connection.setAutoCommit(false);
      stmt = connection.createStatement();
      ResultSet allJobsResultSet = stmt.executeQuery(GET_ALL_JOB_CONFIGURATIONS);

      updateJSONConfigStmt = connection.prepareStatement(UPDATE_SPECIFIC_JOB_JSON_CONFIG);
      while (allJobsResultSet.next()) {
        int id = allJobsResultSet.getInt(1);
        String oldConfig = allJobsResultSet.getString(2);

        LOGGER.info("Trying to migrate JobID: " + id);
        JSONObject config = new JSONObject(oldConfig);
        addKeyValue(config, "NUM_GPUS", "0");
        String newConfig = config.toString();
        LOGGER.info("Successfully migrated JobID: " + id);

        updateJSONConfigStmt.setString(1, newConfig);
        updateJSONConfigStmt.setInt(2, id);
        updateJSONConfigStmt.addBatch();
      }
      updateJSONConfigStmt.executeBatch();
      connection.commit();
      connection.setAutoCommit(true);
    } catch(SQLException ex) {
      String errorMsg = "Could not migrate job configurations";
      LOGGER.error(errorMsg);
      throw new MigrationException(errorMsg, ex);
    } finally {
      closeConnections(stmt, updateJSONConfigStmt);
    }
    LOGGER.info("Finished jobConfig migration");
  }

  private void removeKeyIfExists(JSONObject config, String key) {
    if(config.has(key)) {
      config.remove(key);
    }
  }

  private void addKeyValue(JSONObject config, String key, Object value) {
    config.put(key, value);
  }

  @Override
  public void rollback() throws RollbackException {
    LOGGER.info("Starting jobConfig rollback");
    try {
      setup();
    } catch (SQLException | ConfigurationException ex) {
      String errorMsg = "Could not initialize database connection";
      LOGGER.error(errorMsg);
      throw new RollbackException(errorMsg, ex);
    }

    Statement stmt = null;
    PreparedStatement updateJSONConfigStmt = null;

    try {
      connection.setAutoCommit(false);
      stmt = connection.createStatement();
      ResultSet allJobsResultSet = stmt.executeQuery(GET_ALL_JOB_CONFIGURATIONS);

      updateJSONConfigStmt = connection.prepareStatement(UPDATE_SPECIFIC_JOB_JSON_CONFIG);
      while (allJobsResultSet.next()) {
        int id = allJobsResultSet.getInt(1);
        String oldConfig = allJobsResultSet.getString(2);

        LOGGER.info("Trying to migrate JobID: " + id);
        JSONObject config = new JSONObject(oldConfig);
        removeKeyIfExists(config, "NUM_GPUS");
        String newConfig = config.toString();
        LOGGER.info("Successfully rollbacked JobID: " + id);
        updateJSONConfigStmt.setString(1, newConfig);
        updateJSONConfigStmt.setInt(2, id);
        updateJSONConfigStmt.addBatch();
      }
      updateJSONConfigStmt.executeBatch();
      connection.commit();
      connection.setAutoCommit(true);
    } catch(SQLException ex) {
      String errorMsg = "Could not migrate job configurations";
      LOGGER.error(errorMsg);
      throw new RollbackException(errorMsg, ex);
    } finally {
      closeConnections(stmt, updateJSONConfigStmt);
    }
    LOGGER.info("Finished jobConfig rollback");
  }

  private void closeConnections(Statement stmt, PreparedStatement preparedStatement) {
    try {
      if(stmt != null) {
        stmt.close();
      }
      if(preparedStatement != null) {
        preparedStatement.close();
      }
    } catch(SQLException ex) {
      //do nothing
    }
  }
}
