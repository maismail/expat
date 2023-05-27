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

public class UpdateJobArgsConfiguration implements MigrateStep {
  private final static Logger LOGGER = LoggerFactory.getLogger(UpdateJobArgsConfiguration.class);
  
  private final static String GET_ALL_JOB_CONFIGURATIONS = "SELECT id, json_config FROM jobs";
  private final static String UPDATE_SPECIFIC_JOB_JSON_CONFIG = "UPDATE jobs SET json_config = ? WHERE id = ?";
  private final static String UPDATE_LATEST_EXECUTION_ARGS = "UPDATE executions as e1 JOIN " +
    "(SELECT max(id) as id_to_update, job_id from executions where job_id= ? GROUP BY job_id) as e2 " +
    "SET args=? WHERE e1.id = e2.id_to_update";
  //For rollback
  private final static String SELECT_JOB_ARGS_TO_RESTORE = "select job_id, json_config, args " +
    "from jobs join (select e1.job_id, args from executions as e1 join (" +
    "select max(id) as id_to_update, job_id from executions where job_id= ? group by job_id) as e2 " +
    "on e1.job_id = e2.job_id where args is not null) as jobargs on jobs.id = jobargs.job_id;";
  
  
  protected Connection connection;
  
  private void setup() throws SQLException, ConfigurationException {
    connection = DbConnectionFactory.getConnection();
  }
  
  @Override
  public void migrate() throws MigrationException {
    LOGGER.info("Starting jobConfig args to executions migration");
    try {
      setup();
    } catch (SQLException | ConfigurationException ex) {
      String errorMsg = "Could not initialize database connection";
      LOGGER.error(errorMsg);
      throw new MigrationException(errorMsg, ex);
    }
    
    Statement stmt = null;
    PreparedStatement updateJSONConfigStmt = null;
    PreparedStatement updateExecutionsStmt = null;
    try {
      connection.setAutoCommit(false);
      stmt = connection.createStatement();
      ResultSet allJobsResultSet = stmt.executeQuery(GET_ALL_JOB_CONFIGURATIONS);
      
      updateExecutionsStmt = connection.prepareStatement(UPDATE_LATEST_EXECUTION_ARGS);
      updateJSONConfigStmt = connection.prepareStatement(UPDATE_SPECIFIC_JOB_JSON_CONFIG);
  
      while (allJobsResultSet.next()) {
        int jobId = allJobsResultSet.getInt(1);
        String config = allJobsResultSet.getString(2);
        
        LOGGER.info("Trying to migrate JobID: " + jobId);
        JSONObject configJson = new JSONObject(config);
        if (configJson.get("jobType").equals("SPARK") || configJson.get("jobType").equals("PYSPARK")) {
          //Get job args and update last execution
          String args = convertJSON(configJson, null, true);
          
          updateExecutionsStmt.setInt(1, jobId);
          updateExecutionsStmt.setString(2, args);
          updateExecutionsStmt.addBatch();
  
          updateJSONConfigStmt.setString(1, configJson.toString());
          updateJSONConfigStmt.setInt(2, jobId);
          updateJSONConfigStmt.addBatch();
        }
        LOGGER.info("Successfully migrated JobID: " + jobId);
  
      }
      updateJSONConfigStmt.executeBatch();
      updateExecutionsStmt.executeBatch();
      connection.commit();
      connection.setAutoCommit(true);
    } catch (SQLException ex) {
      String errorMsg = "Could not migrate job configurations";
      LOGGER.error(errorMsg);
      throw new MigrationException(errorMsg, ex);
    } finally {
      closeConnections(stmt, updateJSONConfigStmt);
    }
    LOGGER.info("Finished jobConfig migration");
  }
  
  //Returns the value that was removed
  private String convertJSON(JSONObject configJson, String value, boolean migrate) {
    if (migrate) {
      
      if (configJson.get("jobType").equals("SPARK") || configJson.get("jobType").equals("PYSPARK")) {
        //Get job args and update last execution
        return removeKeyIfExists(configJson, "args");
      }
      
    } else {
      addKeyValue(configJson, "args", value);
    }
    
    return "";
  }
  
  private String removeKeyIfExists(JSONObject config, String key) {
    if (config.has(key)) {
      return (String) config.remove(key);
    }
    return "";
  }
  
  private void addKeyValue(JSONObject configJson, String key, String value) {
    configJson.put(key, value);
  }
  
  @Override
  public void rollback() throws RollbackException {
    LOGGER.info("Starting jobConfig args to executions rollback");
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
      ResultSet jobArgsToRestore = stmt.executeQuery(SELECT_JOB_ARGS_TO_RESTORE);
      
      updateJSONConfigStmt = connection.prepareStatement(UPDATE_SPECIFIC_JOB_JSON_CONFIG);
      while (jobArgsToRestore.next()) {
        int id = jobArgsToRestore.getInt(1);
        JSONObject config = new JSONObject(jobArgsToRestore.getString(2));
        String args = jobArgsToRestore.getString(3);
        
        LOGGER.info("Trying to rollback JobID: " + id);
        convertJSON(config, args, false);
        LOGGER.info("Successfully rollbacked JobID: " + id);
        
        updateJSONConfigStmt.setString(1, config.toString());
        updateJSONConfigStmt.setInt(2, id);
        updateJSONConfigStmt.addBatch();
      }
      updateJSONConfigStmt.executeBatch();
      connection.commit();
      connection.setAutoCommit(true);
    } catch (SQLException ex) {
      String errorMsg = "Could not migrate job configurations";
      LOGGER.error(errorMsg);
      throw new RollbackException(errorMsg, ex);
    } finally {
      closeConnections(stmt, updateJSONConfigStmt);
    }
    LOGGER.info("Starting jobConfig rollback");
  }
  
  private void closeConnections(Statement stmt, PreparedStatement preparedStatement) {
    try {
      if (stmt != null) {
        stmt.close();
      }
      if (preparedStatement != null) {
        preparedStatement.close();
      }
    } catch (SQLException ex) {
      //do nothing
    }
  }
}
