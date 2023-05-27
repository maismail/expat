package io.hops.hopsworks.expat.migrations.jobs;

import com.google.common.base.Strings;
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


public class RenameResources implements MigrateStep {
  private final static Logger LOGGER = LoggerFactory.getLogger(RenameResources.class);

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
    } catch(SQLException ex) {
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

    if(migrate) {

      removeKeyIfExists(config, "kafka");

      migrateResources(config);

      removeKeyIfExists(config, "localResources");

    } else {
      rollbackResources(config);
    }

    return config.toString();
  }

  private void removeKeyIfExists(JSONObject config, String key) {
    if(config.has(key)) {
      config.remove(key);
    }
  }

  private void addKeyValue(JSONObject config, String key, Object value) {
    config.put(key, value);
  }

  private void migrateResources(JSONObject config) {
    StringBuilder pyFiles = new StringBuilder();
    StringBuilder files = new StringBuilder();
    StringBuilder jars = new StringBuilder();
    StringBuilder archives = new StringBuilder();

    if(config.has("localResources")) {
      JSONArray resources = (JSONArray) config.get("localResources");
      for (int i = 0; i < resources.length(); i++) {
        JSONObject topicObj = (JSONObject) resources.get(i);
        //These are needed for migration
        if (topicObj.has("type") && topicObj.has("path")) {
          String type = (String) topicObj.get("type");
          String path = (String) topicObj.get("path");

          if (path.endsWith(".jar")) {
            jars.append(path).append(",");
          } else if (path.endsWith(".py")) {
            pyFiles.append(path).append(",");
          } else if (type.compareToIgnoreCase("archive") == 0) {
            archives.append(path).append(",");
          } else if (type.compareToIgnoreCase("file") == 0) {
            files.append(path).append(",");
          }
        }
      }
      addKeyValue(config, "spark.yarn.dist.jars", jars.toString());
      addKeyValue(config, "spark.yarn.dist.pyFiles", pyFiles.toString());
      addKeyValue(config, "spark.yarn.dist.archives", archives.toString());
      addKeyValue(config, "spark.yarn.dist.files", files.toString());
    }
  }

  private void rollbackResources(JSONObject config) {

    JSONArray localResources = new JSONArray();

    if(config.has("spark.yarn.dist.jars")) {
      String jars = ((String)config.get("spark.yarn.dist.jars")).trim();
      String[] jarArr = jars.split(",");
      for(String jar: jarArr) {
        if(Strings.isNullOrEmpty(jar) || jar.equals(","))
          continue;
        JSONObject jarConfig = new JSONObject();
        String name = jar.substring(jar.lastIndexOf("/") + 1);
        jarConfig.put("name", name);
        jarConfig.put("path", jar);
        jarConfig.put("visibility", "application");
        jarConfig.put("type", "file");
        localResources.put(jarConfig);
      }
    }
    removeKeyIfExists(config,"spark.yarn.dist.jars");

    if(config.has("spark.yarn.dist.files")) {
      String files = ((String)config.get("spark.yarn.dist.files")).trim();
      String[] filesArr = files.split(",");
      for(String file: filesArr) {
        if(Strings.isNullOrEmpty(file) || file.equals(","))
          continue;
        JSONObject fileConfig = new JSONObject();
        String name = file.substring(file.lastIndexOf("/") + 1);
        fileConfig.put("name", name);
        fileConfig.put("path", file);
        fileConfig.put("visibility", "application");
        fileConfig.put("type", "file");
        localResources.put(fileConfig);
      }
    }
    removeKeyIfExists(config,"spark.yarn.dist.files");

    if(config.has("spark.yarn.dist.archives")) {
      String archives = ((String)config.get("spark.yarn.dist.archives")).trim();
      String[] archiveArr = archives.split(",");
      for(String archive: archiveArr) {
        if(Strings.isNullOrEmpty(archive) || archive.equals(","))
          continue;
        JSONObject archiveConfig = new JSONObject();
        String name = archive.substring(archive.lastIndexOf("/") + 1);
        archiveConfig.put("name", name);
        archiveConfig.put("path", archive);
        archiveConfig.put("visibility", "application");
        archiveConfig.put("type", "archive");
        localResources.put(archiveConfig);
      }
    }
    removeKeyIfExists(config,"spark.yarn.dist.archives");

    if(config.has("spark.yarn.dist.pyFiles")) {
      String pyFiles = ((String)config.get("spark.yarn.dist.pyFiles")).trim();
      String[] pyFilesArr = pyFiles.split(",");
      for(String pyFile: pyFilesArr) {
        if(Strings.isNullOrEmpty(pyFile) || pyFile.equals(","))
          continue;
        JSONObject pyFileConfig = new JSONObject();
        String name = pyFile.substring(pyFile.lastIndexOf("/") + 1);
        pyFileConfig.put("name", name);
        pyFileConfig.put("path", pyFile);
        pyFileConfig.put("visibility", "application");
        pyFileConfig.put("type", "file");
        localResources.put(pyFileConfig);
      }
    }
    removeKeyIfExists(config,"spark.yarn.dist.pyFiles");

    config.put("localResources", localResources);
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
    } catch(SQLException ex) {
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
