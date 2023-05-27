package io.hops.hopsworks.expat.migrations.jobs;

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
import java.sql.Statement;
import java.util.Set;

public class UpdateJobConfiguration implements MigrateStep {
  private final static Logger LOGGER = LoggerFactory.getLogger(UpdateJobConfiguration.class);
  
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
        String newConfig = convertJSON(oldConfig, true);
        LOGGER.info("Successfully migrated JobID: " + id);
        
        updateJSONConfigStmt.setString(1, newConfig);
        updateJSONConfigStmt.setInt(2, id);
        updateJSONConfigStmt.addBatch();
      }
      updateJSONConfigStmt.executeBatch();
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
  
  //This function converts an old jobConfig to the new format
  private String convertJSON(String oldConfig, boolean migrate) {
    
    
    JSONObject config = new JSONObject(oldConfig);
    
    if (migrate) {
      
      renameIfKeyExists(config, "type", "jobType");
      
      if (config.get("jobType").equals("SPARK") || config.get("jobType").equals("PYSPARK")) {
        addKeyValue(config, "type", "sparkJobConfiguration");
      } else {
        renameIfKeyExists(config, "jobType", "type");
        return oldConfig.toString();
      }
      
      //These do not exist in new Config, drop them
      removeKeyIfExists(config, "HISTORYSERVER");
      removeKeyIfExists(config, "PYSPARK_PYTHON");
      removeKeyIfExists(config, "PYLIB");
      removeKeyIfExists(config, "DYNEXECSMAX");
      removeKeyIfExists(config, "DYNEXECSMIN");
      removeKeyIfExists(config, "IS_TFONSPARK");
      
      //These were renamed
      renameIfKeyExists(config, "JARPATH", "appPath");
      renameIfKeyExists(config, "ARGS", "args");
      renameIfKeyExists(config, "APPNAME", "appName");
      renameIfKeyExists(config, "MAINCLASS", "mainClass");
      renameIfKeyExists(config, "PROPERTIES", "properties");
      renameIfKeyExists(config, "QUEUE", "amQueue");
      
      renameIfKeyExists(config, "AMMEM", "amMemory");
      renameIfKeyExists(config, "AMCORS", "amVCores");
      
      renameIfKeyExists(config, "EXECMEM", "spark.executor.memory");
      renameIfKeyExists(config, "EXECCORES", "spark.executor.cores");
      renameIfKeyExists(config, "NUM_GPUS", "spark.executor.gpus");
      
      renameIfKeyExists(config, "NUMEXECS", "spark.executor.instances");
      
      renameIfKeyExists(config, "DYNEXECS", "spark.dynamicAllocation.enabled");
      renameIfKeyExists(config, "DYNEXECSMINSELECTED", "spark.dynamicAllocation.minExecutors");
      renameIfKeyExists(config, "DYNEXECSMAXSELECTED", "spark.dynamicAllocation.maxExecutors");
      renameIfKeyExists(config, "DYNEXECSINIT", "spark.dynamicAllocation.initialExecutors");
      
      //If kafka config exists
      if (config.has("KAFKA")) {
        renameKafka(config, true);
      }
      
      if (config.has("SCHEDULE")) {
        renameSchedule(config, true);
      }
      
      if (config.has("RESOURCES")) {
        renameResources(config, true);
      }
    } else {
      
      renameIfKeyExists(config, "jobType", "type");
      
      //These do not exist in new Config, drop them
      addKeyValue(config, "HISTORYSERVER", "");
      addKeyValue(config, "PYSPARK_PYTHON", "");
      addKeyValue(config, "PYLIB", "");
      addKeyValue(config, "DYNEXECSMAX", 1500);
      addKeyValue(config, "DYNEXECSMIN", 1);
      
      //These were renamed
      renameIfKeyExists(config, "appPath", "JARPATH");
      renameIfKeyExists(config, "args", "ARGS");
      renameIfKeyExists(config, "appName", "APPNAME");
      renameIfKeyExists(config, "mainClass", "MAINCLASS");
      renameIfKeyExists(config, "properties", "PROPERTIES");
      renameIfKeyExists(config, "amQueue", "QUEUE");
      
      renameIfKeyExists(config, "amMemory", "AMMEM");
      renameIfKeyExists(config, "amVCores", "AMCORS");
      
      renameIfKeyExists(config, "spark.executor.memory", "EXECMEM");
      renameIfKeyExists(config, "spark.executor.cores", "EXECCORES");
      renameIfKeyExists(config, "spark.executor.gpus", "NUM_GPUS");
      
      renameIfKeyExists(config, "spark.executor.instances", "NUMEXECS");
      
      renameIfKeyExists(config, "spark.dynamicAllocation.enabled", "DYNEXECS");
      renameIfKeyExists(config, "spark.dynamicAllocation.minExecutors", "DYNEXECSMINSELECTED");
      renameIfKeyExists(config, "spark.dynamicAllocation.maxExecutors", "DYNEXECSMAXSELECTED");
      renameIfKeyExists(config, "spark.dynamicAllocation.initialExecutors", "DYNEXECSINIT");
      
      //If kafka config exists
      if (config.has("kafka")) {
        renameKafka(config, false);
      }
      
      if (config.has("schedule")) {
        renameSchedule(config, false);
      }
      
      if (config.has("localResources")) {
        renameResources(config, false);
      }
      
    }
    
    return config.toString();
  }
  
  private void renameIfKeyExists(JSONObject config, String oldKey, String newKey) {
    if (config.has(oldKey)) {
      Object oldKeyValue = config.get(oldKey);
      config.remove(oldKey);
      config.put(newKey, oldKeyValue);
    }
  }
  
  private void removeKeyIfExists(JSONObject config, String key) {
    if (config.has(key)) {
      config.remove(key);
    }
  }
  
  private void addKeyValue(JSONObject config, String key, Object value) {
    config.put(key, value);
  }
  
  private void renameKafka(JSONObject config, boolean migrate) {
    
    if (migrate) {
      
      renameIfKeyExists(config, "KAFKA", "kafka");
      
      JSONObject kafkaObj = (JSONObject) config.get("kafka");
      
      if (kafkaObj.has("TOPICS")) {
        JSONArray topicArr = new JSONArray();
        renameIfKeyExists(kafkaObj, "TOPICS", "topics");
        JSONObject topicObj = (JSONObject) kafkaObj.get("topics");
        Set<String> keys = topicObj.keySet();
        for (String key : keys) {
          JSONObject obj = new JSONObject();
          obj.put("name", ((JSONObject) topicObj.get(key)).get("NAME"));
          obj.put("ticked", ((JSONObject) topicObj.get(key)).get("TICKED"));
          topicArr.put(topicArr.length(), obj);
        }
        kafkaObj.put("topics", topicArr);
      }
      
      
      if (kafkaObj.has("CONSUMER_GROUPS")) {
        JSONArray topicArr = new JSONArray();
        renameIfKeyExists(kafkaObj, "CONSUMER_GROUPS", "consumerGroups");
        JSONObject topicObj = (JSONObject) kafkaObj.get("consumerGroups");
        Set<String> keys = topicObj.keySet();
        for (String key : keys) {
          JSONObject obj = new JSONObject();
          obj.put("name", ((JSONObject) topicObj.get(key)).get("NAME"));
          obj.put("id", ((JSONObject) topicObj.get(key)).get("ID"));
          topicArr.put(topicArr.length(), obj);
        }
        kafkaObj.put("consumerGroups", topicArr);
      }
      
      if (kafkaObj.has("ADVANCED")) {
        renameIfKeyExists(kafkaObj, "ADVANCED", "advanced");
      }
    } else {
      
      renameIfKeyExists(config, "kafka", "KAFKA");
      
      JSONObject kafkaObj = (JSONObject) config.get("KAFKA");
      
      if (kafkaObj.has("topics")) {
        renameIfKeyExists(kafkaObj, "topics", "TOPICS");
        if (kafkaObj.get("TOPICS") instanceof JSONObject) {
          JSONObject topicObj = (JSONObject) kafkaObj.get("TOPICS");
          Set<String> keys = topicObj.keySet();
          for (String key : keys) {
            renameIfKeyExists((JSONObject) topicObj.get(key), "ticked", "TICKED");
            renameIfKeyExists((JSONObject) topicObj.get(key), "name", "NAME");
          }
        } else {
          JSONArray topicsArr = (JSONArray) kafkaObj.get("TOPICS");
          for (int i = 0; i < topicsArr.length(); i++) {
            JSONObject topicObj = (JSONObject) topicsArr.get(i);
            renameIfKeyExists(topicObj, "ticked", "TICKED");
            renameIfKeyExists(topicObj, "name", "NAME");
          }
        }
      }
      
      if (kafkaObj.has("consumerGroups")) {
        renameIfKeyExists(kafkaObj, "consumerGroups", "CONSUMER_GROUPS");
        if (kafkaObj.get("CONSUMER_GROUPS") instanceof JSONObject) {
          JSONObject consumerObj = (JSONObject) kafkaObj.get("CONSUMER_GROUPS");
          renameIfKeyExists(consumerObj, "id", "ID");
          renameIfKeyExists(consumerObj, "name", "NAME");
        } else {
          JSONArray consumerArr = (JSONArray) kafkaObj.get("CONSUMER_GROUPS");
          for (int i = 0; i < consumerArr.length(); i++) {
            JSONObject topicObj = (JSONObject) consumerArr.get(i);
            renameIfKeyExists(topicObj, "id", "ID");
            renameIfKeyExists(topicObj, "name", "NAME");
          }
        }
      }
      
      if (kafkaObj.has("advanced")) {
        renameIfKeyExists(kafkaObj, "advanced", "ADVANCED");
      }
      
    }
  }
  
  private void renameSchedule(JSONObject config, boolean migrate) {
    if (migrate) {
      renameIfKeyExists(config, "SCHEDULE", "schedule");
      
      JSONObject scheduleObj = (JSONObject) config.get("schedule");
      
      if (scheduleObj.has("NUMBER")) {
        renameIfKeyExists(scheduleObj, "NUMBER", "number");
      }
      
      if (scheduleObj.has("UNIT")) {
        renameIfKeyExists(scheduleObj, "UNIT", "unit");
      }
      
      if (scheduleObj.has("START")) {
        renameIfKeyExists(scheduleObj, "START", "start");
      }
    } else {
      renameIfKeyExists(config, "schedule", "SCHEDULE");
      
      JSONObject scheduleObj = (JSONObject) config.get("SCHEDULE");
      
      if (scheduleObj.has("number")) {
        renameIfKeyExists(scheduleObj, "number", "NUMBER");
      }
      
      if (scheduleObj.has("unit")) {
        renameIfKeyExists(scheduleObj, "unit", "UNIT");
      }
      
      if (scheduleObj.has("start")) {
        renameIfKeyExists(scheduleObj, "start", "START");
      }
    }
  }
  
  private void renameResources(JSONObject config, boolean migrate) {
    if (migrate) {
      if (config.has("RESOURCES")) {
        JSONArray resourcesArr = new JSONArray();
        renameIfKeyExists(config, "RESOURCES", "localResources");
        JSONObject resourcesObj = (JSONObject) config.get("localResources");
        Set<String> keys = resourcesObj.keySet();
        for (String key : keys) {
          JSONObject obj = new JSONObject();
          obj.put("name", ((JSONObject) resourcesObj.get(key)).get("NAME"));
          obj.put("path", ((JSONObject) resourcesObj.get(key)).get("PATH"));
          obj.put("visibility", ((JSONObject) resourcesObj.get(key)).get("VISIBILITY"));
          obj.put("type", ((JSONObject) resourcesObj.get(key)).get("TYPE"));
          resourcesArr.put(resourcesArr.length(), obj);
        }
        config.put("localResources", resourcesArr);
      }
    } else {
      renameIfKeyExists(config, "localResources", "RESOURCES");
      JSONObject resourcesObj = config.getJSONObject("RESOURCES");
      Set<String> keys = resourcesObj.keySet();
      for (String key : keys) {
        renameIfKeyExists((JSONObject) resourcesObj.get(key), "name", "NAME");
        renameIfKeyExists((JSONObject) resourcesObj.get(key), "path", "PATH");
        renameIfKeyExists((JSONObject) resourcesObj.get(key), "visibility", "VISIBILITY");
        renameIfKeyExists((JSONObject) resourcesObj.get(key), "type", "TYPE");
        renameIfKeyExists((JSONObject) resourcesObj.get(key), "pattern", "PATTERN");
      }
    }
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
        
        LOGGER.info("Trying to rollback JobID: " + id);
        String newConfig = convertJSON(oldConfig, false);
        LOGGER.info("Successfully rollbacked JobID: " + id);
        
        updateJSONConfigStmt.setString(1, newConfig);
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
