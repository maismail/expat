package io.hops.hopsworks.expat.migrations.featurestore.storageconnectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class StorageConnectorMigration implements MigrateStep {
  private static final Logger LOGGER = LoggerFactory.getLogger(StorageConnectorMigration.class);
  
  protected Connection connection;
  DistributedFileSystemOps dfso = null;
  private boolean dryRun;
  private String hopsUser;
  
  private ObjectMapper objectMapper = new ObjectMapper();
  
  private final static String STORAGE_CONNECTORS_RESOURCE_SUBDIR= "storage_connector_resources";
  private final static String FEATURESTORE_HIVE_DB_DIR = "hdfs:///apps/hive/warehouse/%s_featurestore.db";
  
  private final static String GET_ALL_SNOWFLAKE_CONNECTORS =
    "SELECT id, arguments FROM feature_store_snowflake_connector";
  private final static String UPDATE_SNOWFLAKE_ARGUMENTS =
    "UPDATE feature_store_snowflake_connector SET arguments = ? WHERE id = ?";
  private final static String GET_ALL_REDSHIFT_CONNECTORS =
    "SELECT id, arguments FROM feature_store_redshift_connector";
  private final static String UPDATE_REDSHIFT_ARGUMENTS =
    "UPDATE feature_store_redshift_connector SET arguments = ? WHERE id = ?";
  private final static String GET_ALL_JDBC_CONNECTORS =
    "SELECT id, arguments FROM feature_store_jdbc_connector";
  private final static String UPDATE_JDBC_ARGUMENTS =
    "UPDATE feature_store_jdbc_connector SET arguments = ? WHERE id = ?";
  private final static String GET_PROJECT_NAMES = "SELECT projectname FROM project";
  
  private void setup() throws ConfigurationException, SQLException {
    connection = DbConnectionFactory.getConnection();
    
    Configuration conf = ConfigurationBuilder.getConfiguration();
    hopsUser = conf.getString(ExpatConf.HOPS_CLIENT_USER);
    if (hopsUser == null) {
      throw new ConfigurationException(ExpatConf.HOPS_CLIENT_USER + " cannot be null");
    }
    dfso = HopsClient.getDFSO(hopsUser);
    dryRun = conf.getBoolean(ExpatConf.DRY_RUN);
  }
  
  private void close() {
    if(connection != null) {
      try {
        connection.close();
      } catch (SQLException ex) {
        LOGGER.error("failed to close jdbc connection", ex);
      }
    }
    if(dfso != null) {
      dfso.close();
    }
  }
  
  @Override
  public void migrate() throws MigrationException {
    LOGGER.info("Starting storage connector migration");
    
    try {
      setup();
    } catch (ConfigurationException | SQLException ex) {
      String errorMsg = "Could not initialize database connection";
      LOGGER.error(errorMsg);
      close();
      throw new MigrationException(errorMsg, ex);
    }
    
    migrateSnowflakeOptions();
    migrateRedshiftOptions();
    migrateJDBCOptions();
    migrateConnectorResourcesDirectory();
    
    close();
  
    LOGGER.info("Finished storage connector migration");
  }
  
  @Override
  public void rollback() throws RollbackException {
    LOGGER.info("Starting storage connector rollback");
    try {
      setup();
    } catch (ConfigurationException | SQLException ex) {
      String errorMsg = "Could not initialize database connection";
      LOGGER.error(errorMsg);
      close();
      throw new RollbackException(errorMsg, ex);
    }
  
    rollbackSnowflakeOptions();
    rollbackRedshiftOptions();
    rollbackJDBCOptions();
    rollbackConnectorResourcesDirectory();
    
    close();

    LOGGER.info("Finished storage connector rollback");
  }
  
  private void migrateSnowflakeOptions() throws MigrationException {
    migrateConnectorOptions(GET_ALL_SNOWFLAKE_CONNECTORS, UPDATE_SNOWFLAKE_ARGUMENTS, "snowflake");
  }
  
  private void migrateRedshiftOptions() throws MigrationException {
    migrateConnectorOptions(GET_ALL_REDSHIFT_CONNECTORS, UPDATE_REDSHIFT_ARGUMENTS, "redshift");
  }
  
  private void migrateJDBCOptions() throws MigrationException {
    migrateConnectorOptions(GET_ALL_JDBC_CONNECTORS, UPDATE_JDBC_ARGUMENTS, "jdbc");
  }
  
  
  private void migrateConnectorOptions(String getConnectorsSql, String updateConnectorSql, String connector)
      throws MigrationException {
    LOGGER.info("Starting to migrate " + connector + " Connector Options");
  
    try {
      connection.setAutoCommit(false);
    
      PreparedStatement getStatement = connection.prepareStatement(getConnectorsSql);
      PreparedStatement updateStatement = connection.prepareStatement(updateConnectorSql);
      ResultSet connectorArguments = getStatement.executeQuery();
    
      int currentConnectorId;
      String currentArguments;
      List<OptionDTO> arguments;
    
      while (connectorArguments.next()) {
        currentArguments = connectorArguments.getString("arguments");
        currentConnectorId = connectorArguments.getInt("id");
        
        try {
          // for idempotency try first to deserialize with new format
          arguments = toOptions(currentArguments);
        } catch (MigrationException e) {
          if (connector.equals("jdbc")) {
            arguments = oldToOptions(currentArguments, ",");
          } else {
            arguments = oldToOptions(currentArguments, ";");
          }
        }
      
        if (!dryRun) {
          updateStatement.setString(1, fromOptions(arguments));
          updateStatement.setInt(2, currentConnectorId);
          updateStatement.execute();
        }
      }
      getStatement.close();
      updateStatement.close();
      connection.commit();
    
      connection.setAutoCommit(true);
    } catch (SQLException e) {
      close();
      throw new MigrationException("error", e);
    }
    LOGGER.info("Finished to migrate " + connector + " Connector Options");
  }
  
  private void migrateConnectorResourcesDirectory() throws MigrationException {
    LOGGER.info("Starting to migrate connector resources directory");
  
    try {
      connection.setAutoCommit(false);

      PreparedStatement projectNamesStatement = connection.prepareStatement(GET_PROJECT_NAMES);
      ResultSet projectNamesResultSet = projectNamesStatement.executeQuery();

      String currentProjectName;

      while (projectNamesResultSet.next()) {
        // check if project is feature store enabled by checking if the feature store hive db exists
        currentProjectName = projectNamesResultSet.getString("projectname");
        Path featureStorePath = new Path(String.format(FEATURESTORE_HIVE_DB_DIR, currentProjectName));
        if (dfso.exists(featureStorePath)) {
          FileStatus fileStatus = dfso.getFileStatus(featureStorePath);
          FsPermission featureStoreDbPermissions = fileStatus.getPermission();
          String owner = fileStatus.getOwner();
          String group = fileStatus.getGroup();
          Path storageConnectorResourcePath = new Path(featureStorePath + "/" + STORAGE_CONNECTORS_RESOURCE_SUBDIR);
          if (!dryRun && !dfso.exists(storageConnectorResourcePath)) {
            dfso.mkdir(storageConnectorResourcePath, featureStoreDbPermissions);
            dfso.setOwner(storageConnectorResourcePath, owner, group);
          }
        }
      }

      connection.commit();
      connection.setAutoCommit(true);
    } catch (SQLException | IOException e) {
      close();
      throw new MigrationException("error", e);
    }
    
    LOGGER.info("Finished to migrate connector resources directory");
  }
  
  private void rollbackSnowflakeOptions() throws RollbackException {
    rollbackConnectorOptions(GET_ALL_SNOWFLAKE_CONNECTORS, UPDATE_SNOWFLAKE_ARGUMENTS, "snowflake");
  }
  
  private void rollbackRedshiftOptions() throws RollbackException {
    rollbackConnectorOptions(GET_ALL_REDSHIFT_CONNECTORS, UPDATE_REDSHIFT_ARGUMENTS, "redshift");
  }
  
  private void rollbackJDBCOptions() throws RollbackException {
    rollbackConnectorOptions(GET_ALL_JDBC_CONNECTORS, UPDATE_JDBC_ARGUMENTS, "jdbc");
  }
  
  private void rollbackConnectorOptions(String getConnectorSql, String updateConnectorSql, String connector)
      throws RollbackException {
    LOGGER.info("Starting to rollback " + connector + " Connector Options");
    
    try {
      connection.setAutoCommit(false);
      
      PreparedStatement getStatement = connection.prepareStatement(getConnectorSql);
      PreparedStatement updateStatement = connection.prepareStatement(updateConnectorSql);
      ResultSet connectorArguments = getStatement.executeQuery();
      
      int currentConnectorId;
      String currentArguments;
      List<OptionDTO> arguments;
      
      while (connectorArguments.next()) {
        currentArguments = connectorArguments.getString("arguments");
        currentConnectorId = connectorArguments.getInt("id");
        
        try {
          // for idempotency try first to deserialize with new format
          arguments = toOptions(currentArguments);
        } catch (MigrationException e) {
          if (connector.equals("jdbc")) {
            arguments = oldToOptions(currentArguments, ",");
          } else {
            arguments = oldToOptions(currentArguments, ";");
          }
        }
        
        if (!dryRun) {
          if (connector.equals("jdbc")) {
            updateStatement.setString(1, oldFromOptions(arguments, ","));
          } else {
            updateStatement.setString(1, oldFromOptions(arguments, ";"));
          }
          updateStatement.setInt(2, currentConnectorId);
          updateStatement.execute();
        }
      }
      getStatement.close();
      updateStatement.close();
      connection.commit();
      
      connection.setAutoCommit(true);
    } catch (SQLException e) {
      close();
      throw new RollbackException("error", e);
    }
    LOGGER.info("Finished to rollback " + connector + " Connector Options");
  }
  
  private void rollbackConnectorResourcesDirectory() throws RollbackException {
    LOGGER.info("Starting to rollback connector resources directory");
    
    try {
      connection.setAutoCommit(false);
      
      PreparedStatement projectNamesStatement = connection.prepareStatement(GET_PROJECT_NAMES);
      ResultSet projectNamesResultSet = projectNamesStatement.executeQuery();
      
      String currentProjectName;
      
      while (projectNamesResultSet.next()) {
        // check if project is feature store enabled by checking if the feature store hive db exists
        currentProjectName = projectNamesResultSet.getString("projectname");
        Path featureStorePath = new Path(String.format(FEATURESTORE_HIVE_DB_DIR, currentProjectName));
        if (dfso.exists(featureStorePath)) {
          Path storageConnectorResourcePath = new Path(featureStorePath + "/" + STORAGE_CONNECTORS_RESOURCE_SUBDIR);
          if (!dryRun && dfso.exists(storageConnectorResourcePath)) {
            dfso.rm(storageConnectorResourcePath, true);
          }
        }
      }
      
      connection.commit();
      connection.setAutoCommit(true);
    } catch (SQLException | IOException e) {
      close();
      throw new RollbackException("error", e);
    }
    
    LOGGER.info("Finished to rollback connector resources directory");
  }
  
  private List<OptionDTO> oldToOptions(String arguments, String separator) {
    if (Strings.isNullOrEmpty(arguments) || arguments.equals("[{}]") || arguments.equals("null")) {
      return null;
    }
    List<OptionDTO> optionList = Arrays.stream(arguments.split(separator))
      .map(arg -> arg.split("="))
      .map(a -> (a.length > 1) ? new OptionDTO(a[0], a[1]) : new OptionDTO(a[0], null))
      .collect(Collectors.toList());
    if (dryRun) {
      LOGGER.info("Old arguments string: " + arguments);
      LOGGER.info("Deserialized options: " + optionList);
    }
    return optionList;
  }
  
  private String oldFromOptions(List<OptionDTO> options, String separator) {
    if (options == null || options.isEmpty()) {
      return null;
    }
    StringBuilder arguments = new StringBuilder();
    for (OptionDTO option : options) {
      arguments.append(arguments.length() > 0? separator : "")
        .append(option.getName())
        .append(option.getValue() != null ? "=" + option.getValue() : "");
    }
    if (dryRun) {
      LOGGER.info("Old Deserialized Options: " + options);
      LOGGER.info("Rolling back to string: " + arguments);
    }
    return arguments.toString();
  }
  
  // new toOptions
  public List<OptionDTO> toOptions(String arguments) throws MigrationException {
    if (Strings.isNullOrEmpty(arguments) || arguments.equals("[{}]") || arguments.equals("null")) {
      return null;
    }
    
    try {
      OptionDTO[] optionArray = objectMapper.readValue(arguments, OptionDTO[].class);
      return Arrays.asList(optionArray);
    } catch (JsonProcessingException e) {
      throw new MigrationException("error", e);
    }
  }
  
  // new fromOptions
  public String fromOptions(List<OptionDTO> options) {
    if (options == null || options.isEmpty()) {
      return null;
    }
    return new JSONArray(options).toString();
  }
}
