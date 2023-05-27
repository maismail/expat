/*
 * This file is part of Hopsworks
 * Copyright (C) 2022, Logical Clocks AB. All rights reserved
 *
 * Hopsworks is free software: you can redistribute it and/or modify it under the terms of
 * the GNU Affero General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 *
 * Hopsworks is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this program.
 * If not, see <https://www.gnu.org/licenses/>.
 */

package io.hops.hopsworks.expat.migrations.featurestore.trainingdataset;

import io.hops.hopsworks.common.hdfs.DistributedFileSystemOps;
import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import io.hops.hopsworks.expat.db.DbConnectionFactory;
import io.hops.hopsworks.expat.db.dao.hdfs.inode.ExpatHdfsInode;
import io.hops.hopsworks.expat.db.dao.hdfs.inode.ExpatInodeController;
import io.hops.hopsworks.expat.migrations.MigrateStep;
import io.hops.hopsworks.expat.migrations.MigrationException;
import io.hops.hopsworks.expat.migrations.RollbackException;
import io.hops.hopsworks.expat.migrations.projects.util.HopsClient;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class CreateExternalTrainingDatasetInode implements MigrateStep {
  private static final Logger LOGGER = LoggerFactory.getLogger(CreateExternalTrainingDatasetInode.class);
  
  protected Connection connection;
  DistributedFileSystemOps dfso = null;
  private boolean dryRun;
  private String hopsUser;
  private ExpatInodeController inodeController;
  
  private final static String PROJECT_TRAINING_DATASETS_DIR = "/Projects/%s/%s_Training_Datasets";
  
  private final static String GET_ALL_EXTERNAL_TRAINING_DATASETS =
    "SELECT a.id, a.feature_store_id, a.name, a.version, a.external_training_dataset_id, b.project_id, c.projectname," +
      " d.username " +
      "FROM training_dataset AS a " +
      "INNER JOIN feature_store AS b ON a.feature_store_id = b.id " +
      "INNER JOIN project AS c ON b.project_id = c.id " +
      "INNER JOIN users AS d on a.creator = d.uid " +
      "WHERE external_training_dataset_id IS NOT NULL";
  private final static String UPDATE_EXTERNAL_TRAINING_DATASET_INODE =
    "UPDATE external_training_dataset " +
      "SET inode_pid = ?, " +
      "inode_name = ?, " +
      "partition_id = ? " +
      "WHERE id = ?";
  
  private void setup()
    throws ConfigurationException, SQLException {
    connection = DbConnectionFactory.getConnection();
    
    Configuration conf = ConfigurationBuilder.getConfiguration();
    hopsUser = conf.getString(ExpatConf.HOPS_CLIENT_USER);
    if (hopsUser == null) {
      throw new ConfigurationException(ExpatConf.HOPS_CLIENT_USER + " cannot be null");
    }
    dfso = HopsClient.getDFSO(hopsUser);
    dryRun = conf.getBoolean(ExpatConf.DRY_RUN);
    inodeController = new ExpatInodeController(this.connection);
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
    LOGGER.info("Starting external training dataset migration");
    
    try {
      setup();
    } catch (ConfigurationException | SQLException ex) {
      String errorMsg = "Could not initialize database connection";
      LOGGER.error(errorMsg);
      close();
      throw new MigrationException(errorMsg, ex);
    }
    
    migrateExternalTrainingDatasetInode();
    close();
    
    LOGGER.info("Finished external training dataset migration");
  }
  
  @Override
  public void rollback() throws RollbackException {
    LOGGER.info("Starting external training dataset rollback");
    try {
      setup();
    } catch (ConfigurationException | SQLException ex) {
      String errorMsg = "Could not initialize database connection";
      LOGGER.error(errorMsg);
      close();
      throw new RollbackException(errorMsg, ex);
    }
    
    rollbackExternalTrainingDatasetInode();
    close();
    
    LOGGER.info("Finished external training dataset rollback");
  }
  
  private void migrateExternalTrainingDatasetInode() throws MigrationException {
    try {
      connection.setAutoCommit(false);
    
      PreparedStatement getTrainingDatasetsStatement = connection.prepareStatement(GET_ALL_EXTERNAL_TRAINING_DATASETS);
      PreparedStatement updateExternalTrainingDatasetStatement =
        connection.prepareStatement(UPDATE_EXTERNAL_TRAINING_DATASET_INODE);
      ResultSet trainingDatasets = getTrainingDatasetsStatement.executeQuery();
    
      int externalTrainingDatasetId;
      int trainingDatasetVersion;
      String projectName;
      String trainingDatasetName;
      String userName;
      ExpatHdfsInode externalTdInode;
      
      while (trainingDatasets.next()) {
        externalTrainingDatasetId = trainingDatasets.getInt("external_training_dataset_id");
        projectName = trainingDatasets.getString("projectname");
        trainingDatasetName = trainingDatasets.getString("name");
        trainingDatasetVersion = trainingDatasets.getInt("version");
        userName = trainingDatasets.getString("username");
        
        String projectTdDir = String.format(PROJECT_TRAINING_DATASETS_DIR, projectName, projectName);
        String projectTdDirHdfs = "hdfs://" + projectTdDir;
        Path tdPath = new Path(projectTdDir,trainingDatasetName + "_" + trainingDatasetVersion);
        Path tdPathHdfs = new Path(projectTdDirHdfs,trainingDatasetName + "_" + trainingDatasetVersion);
  
        if (!dfso.exists(tdPath) && !dryRun) {
          FileStatus fileStatus = dfso.getFileStatus(new Path(projectTdDirHdfs));
          FsPermission tdPermissions = fileStatus.getPermission();
          
          dfso.mkdir(tdPathHdfs, tdPermissions);
          String owner = projectName + "__" + userName;
          String group = projectName + "__" + projectName + "_Training_Datasets";
  
          dfso.setOwner(tdPathHdfs, owner, group);
        }
        externalTdInode = inodeController.getInodeAtPath(tdPath.toString());
        if (!dryRun) {
          updateExternalTrainingDatasetStatement.setLong(1, externalTdInode.getParentId());
          updateExternalTrainingDatasetStatement.setString(2, externalTdInode.getName());
          updateExternalTrainingDatasetStatement.setLong(3, externalTdInode.getPartitionId());
          updateExternalTrainingDatasetStatement.setInt(4, externalTrainingDatasetId);
          
          updateExternalTrainingDatasetStatement.execute();
        }
      }
      getTrainingDatasetsStatement.close();
      updateExternalTrainingDatasetStatement.close();
      connection.commit();
    
      connection.setAutoCommit(true);
    } catch (SQLException | IOException e) {
      close();
      throw new MigrationException("error", e);
    }
  }
  
  private void rollbackExternalTrainingDatasetInode() throws RollbackException {
    try {
      connection.setAutoCommit(false);
    
      PreparedStatement getTrainingDatasetsStatement = connection.prepareStatement(GET_ALL_EXTERNAL_TRAINING_DATASETS);
      ResultSet trainingDatasets = getTrainingDatasetsStatement.executeQuery();
    
      int trainingDatasetVersion;
      String projectName;
      String trainingDatasetName;
    
      while (trainingDatasets.next()) {
        projectName = trainingDatasets.getString("projectname");
        trainingDatasetName = trainingDatasets.getString("name");
        trainingDatasetVersion = trainingDatasets.getInt("version");
      
        Path tdPath = new Path("hdfs://" + String.format(PROJECT_TRAINING_DATASETS_DIR, projectName, projectName),
          trainingDatasetName + "_" + trainingDatasetVersion);
        
        if (dfso.exists(tdPath) && !dryRun) {
          // This assumes that the foreign key has been removed before this is being run.
          dfso.rm(tdPath.toString(), true);
        }
      }
      getTrainingDatasetsStatement.close();
      connection.commit();
    
      connection.setAutoCommit(true);
    } catch (SQLException | IOException e) {
      close();
      throw new RollbackException("error", e);
    }
  }
}
