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

package io.hops.hopsworks.expat.migrations.serving;

import io.hops.hopsworks.common.hdfs.DistributedFileSystemOps;
import io.hops.hopsworks.common.hdfs.HdfsUsersController;
import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import io.hops.hopsworks.expat.db.DbConnectionFactory;
import io.hops.hopsworks.expat.db.dao.user.ExpatUser;
import io.hops.hopsworks.expat.db.dao.user.ExpatUserFacade;
import io.hops.hopsworks.expat.db.dao.util.ExpatVariables;
import io.hops.hopsworks.expat.db.dao.util.ExpatVariablesFacade;
import io.hops.hopsworks.expat.migrations.MigrateStep;
import io.hops.hopsworks.expat.migrations.MigrationException;
import io.hops.hopsworks.expat.migrations.RollbackException;
import io.hops.hopsworks.expat.migrations.projects.util.HopsClient;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Stack;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ModelArtifactMigration implements MigrateStep {
  private static final Logger LOGGER = LogManager.getLogger(ModelArtifactMigration.class);
  
  protected Connection connection;
  private boolean dryRun;
  private String hopsUser;
  private ExpatVariablesFacade expatVariablesFacade;
  private ExpatUserFacade expatUserFacade;
  
  private final static String GET_PROJECT_NAMES = "SELECT projectname FROM project";
  private final static String GET_SERVINGS = "SELECT id, model_path, model_version, creator, kafka_topic_id FROM " +
    "serving";
  private final static String UPDATE_SERVING = "UPDATE serving SET artifact_version = ?, inference_logging = ? WHERE " +
    "id = ?";
  
  private final static String MODELS_PATH = "/Projects/%s/Models";
  private final static String MODEL_PATH = MODELS_PATH + "/%s";
  private final static String MODEL_VERSION_PATH = MODEL_PATH + "/%s";
  private final static String ARTIFACTS_PATH = MODEL_VERSION_PATH + "/Artifacts";
  private final static String NEW_ARTIFACT_NAME = "%s_%s_0.zip";
  private final static String OLD_ARTIFACT_NAME = "%s.zip";
  
  @Override
  public void migrate() throws MigrationException {
    LOGGER.info("Starting model artifacts migration");
    
    try {
      setup();
    } catch (SQLException | ConfigurationException ex) {
      String errorMsg = "Could not initialize database connection";
      LOGGER.error(errorMsg);
      throw new MigrationException(errorMsg, ex);
    }
    
    try {
      // check kubernetes is installed
      ExpatVariables kubernetesInstalled = expatVariablesFacade.findById("kubernetes_installed");
      if (!Boolean.parseBoolean(kubernetesInstalled.getValue())) {
        return; // model artifacts are only created with Kubernetes installed
      }
    } catch (IllegalAccessException | SQLException | InstantiationException ex) {
      String errorMsg = "Could not migrate model artifact";
      LOGGER.error(errorMsg);
      throw new MigrationException(errorMsg, ex);
    }
    
    PreparedStatement getProjectNamesStmt = null;
    PreparedStatement getServingsStmt = null;
    PreparedStatement updateServingStmt = null;
    DistributedFileSystemOps dfso = null;
    try {
      connection.setAutoCommit(false);
      dfso = HopsClient.getDFSO(hopsUser);
      
      // Delete old artifacts
      // -- per project
      getProjectNamesStmt = connection.prepareStatement(GET_PROJECT_NAMES);
      ResultSet projectNamesResultSet = getProjectNamesStmt.executeQuery();
      while(projectNamesResultSet.next()) {
        String projectName = projectNamesResultSet.getString(1);
        deleteOldArtifacts(projectName, dfso); // artifacts were created in the model version directory in v2.2
      }
      
      // Create new artifacts
      // -- per serving
      updateServingStmt = connection.prepareStatement(UPDATE_SERVING);
      getServingsStmt = connection.prepareStatement(GET_SERVINGS);
      ResultSet servingsResultSet = getServingsStmt.executeQuery();
      HashSet<String> createdArtifacts = new HashSet<>();
      while(servingsResultSet.next()) {
        // parse query result
        int servingId = servingsResultSet.getInt(1);
        String modelPath = servingsResultSet.getString(2);
        String modelVersion = servingsResultSet.getString(3);
        int creatorUid = servingsResultSet.getInt(4);
        Integer kafkaTopicId = servingsResultSet.getObject(5, Integer.class);
        
        // create the artifact if it has not been created yet for this model version
        String modelPathAndVersion = String.format("%s/%s", modelPath, modelVersion);
        if (!createdArtifacts.contains(modelPathAndVersion)) {
          // build paths
          String[] segments = modelPath.split("/"); // /Projects/<projectName>/Models/<modelName>...
          String projectName = segments[2];
          String modelName = segments[4];
          String modelVersionDir = String.format(MODEL_VERSION_PATH, projectName, modelName, modelVersion);
          String artifactVersionDir = String.format("%s/Artifacts/0", modelVersionDir);
          String artifactFile = String.format("%s/" + NEW_ARTIFACT_NAME, artifactVersionDir, modelName, modelVersion);
          Path modelVersionPath = new Path(modelVersionDir);
          Path artifactVersionDirPath = new Path(artifactVersionDir);
          Path artifactPath = new Path(artifactFile);
  
          // get hdfsuser, permissions, username and group for the artifact files
          ExpatUser creator = expatUserFacade.getExpatUserByUid(connection, creatorUid);
          String username = creator.getUsername();
          String hdfsUser = getHdfsUserName(projectName, username);
          FsPermission artifactPermission = dfso.getParentPermission(modelVersionPath);
          String group = dfso.getFileStatus(modelVersionPath.getParent()).getGroup();
  
          // copy model files to artifact folder
          copyFilesToArtifactFolder(modelVersionPath, artifactVersionDirPath, artifactPermission, hdfsUser, group,
            dfso);
  
          // create new artifact
          createArtifact(artifactVersionDirPath, artifactPath, artifactPermission, hdfsUser, group, dfso);
  
          // add model version artifact as created
          createdArtifacts.add(modelPathAndVersion);
        }
        
        // add serving update to batch
        updateServingStmt.setInt(1, 0); // artifact_version -> 0
        updateServingStmt.setObject(2, kafkaTopicId == null ? null : 2); // inference_logging
        updateServingStmt.setInt(3, servingId);
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
    } catch(IllegalAccessException | InstantiationException | IllegalStateException | IOException | SQLException ex) {
      String errorMsg = "Could not migrate model artifact";
      LOGGER.error(errorMsg);
      throw new MigrationException(errorMsg, ex);
    } finally {
      closeConnections(getProjectNamesStmt, getServingsStmt, updateServingStmt);
      if (dfso != null) {
        dfso.close();
      }
    }
    LOGGER.info("Finished model artifacts migration");
  }
  
  @Override
  public void rollback() throws RollbackException {
    LOGGER.info("Starting model artifacts rollback");
  
    try {
      setup();
    } catch (SQLException | ConfigurationException ex) {
      String errorMsg = "Could not initialize database connection";
      LOGGER.error(errorMsg);
      throw new RollbackException(errorMsg, ex);
    }
  
    try {
      // check kubernetes is installed
      ExpatVariables kubernetesInstalled = expatVariablesFacade.findById("kubernetes_installed");
      if (!Boolean.parseBoolean(kubernetesInstalled.getValue())) {
        return; // model artifacts are only created with Kubernetes installed
      }
    } catch (IllegalAccessException | SQLException | InstantiationException ex) {
      String errorMsg = "Could not migrate model artifact";
      LOGGER.error(errorMsg);
      throw new RollbackException(errorMsg, ex);
    }
  
    PreparedStatement getProjectNamesStmt = null;
    DistributedFileSystemOps dfso = null;
    try {
      dfso = HopsClient.getDFSO(hopsUser);
    
      // Delete new artifacts
      // -- per project
      getProjectNamesStmt = connection.prepareStatement(GET_PROJECT_NAMES);
      ResultSet projectNamesResultSet = getProjectNamesStmt.executeQuery();
      while(projectNamesResultSet.next()) {
        String projectName = projectNamesResultSet.getString(1);
        deleteNewArtifacts(projectName, dfso); // artifacts directory was added in v2.3
      }
    
      // In Hopsworks v2.2, artifacts are created when the serving is started. Therefore, we don't need to recreate
      // old artifacts. Additionally, we don't need to update servings since no attributes were deleted in v2.3.
      
    } catch(IllegalStateException | IOException | SQLException ex) {
      String errorMsg = "Could not rollback model artifact";
      LOGGER.error(errorMsg);
      throw new RollbackException(errorMsg, ex);
    } finally {
      closeConnections(getProjectNamesStmt);
      if (dfso != null) {
        dfso.close();
      }
    }
    LOGGER.info("Finished model artifacts migration");
  }
  
  private void deleteOldArtifacts(String projectName, DistributedFileSystemOps dfso) throws IOException {
    Path modelsPath = new Path(String.format(MODELS_PATH, projectName));
    if(!dfso.exists(modelsPath)){
      LOGGER.info("Project " + projectName + " doesn't have models directory.");
      return;
    }
    // -- per model
    for (FileStatus modelDir : dfso.listStatus(modelsPath)) {
      if (!modelDir.isDirectory()) {
        continue; // ignore files
      }
      String modelName = modelDir.getPath().getName();
    
      // -- per model version
      for (FileStatus modelVersionDir :
        dfso.listStatus(new Path(String.format(MODEL_PATH, projectName, modelName)))) {
        if (!modelVersionDir.isDirectory()) {
          continue; // ignore files
        }
        
        String modelVersion = modelVersionDir.getPath().getName();
        try {
          Integer.parseInt(modelVersion);
        } catch (NumberFormatException nfe) {
          continue; // ignore other than model version directories
        }
        
        // delete old artifact
        Path oldArtifactPath =
          new Path(String.format(MODEL_VERSION_PATH + "/" + OLD_ARTIFACT_NAME, projectName, modelName, modelVersion,
            modelVersion));
        if (dryRun) {
          LOGGER.info("Delete old artifact file: " + oldArtifactPath.toString());
        } else {
          dfso.rm(oldArtifactPath, false);
        }
      }
    }
  }
  
  private void deleteNewArtifacts(String projectName, DistributedFileSystemOps dfso) throws IOException {
    Path modelsPath = new Path(String.format(MODELS_PATH, projectName));
    if(!dfso.exists(modelsPath)){
      LOGGER.info("Project " + projectName + " doesn't have models directory.");
      return;
    }
    // -- per model
    for (FileStatus modelDir : dfso.listStatus(modelsPath)) {
      if (!modelDir.isDirectory()) {
        continue; // ignore files
      }
      String modelName = modelDir.getPath().getName();
      
      // -- per model version
      for (FileStatus modelVersionDir :
        dfso.listStatus(new Path(String.format(MODEL_PATH, projectName, modelName)))) {
        if (!modelVersionDir.isDirectory()) {
          continue; // ignore files
        }
        
        String modelVersion = modelVersionDir.getPath().getName();
        try {
          Integer.parseInt(modelVersion);
        } catch (NumberFormatException nfe) {
          continue; // ignore other than model version directories
        }
        
        // delete artifacts directory and their content
        Path artifactsPath =
          new Path(String.format(ARTIFACTS_PATH, projectName, modelName, modelVersion));
        if (dryRun) {
          LOGGER.info("Remove new artifact directory: " + artifactsPath.toString());
        } else {
          dfso.rm(artifactsPath, true);
        }
      }
    }
  }
  
  private void copyFilesToArtifactFolder(Path modelVersionPath, Path artifactVersionDirPath,
    FsPermission artifactPermission, String username, String group, DistributedFileSystemOps dfso) throws IOException {
    
    // create artifact directory
    if (dryRun) {
      LOGGER.info("Make artifacts directory: "+ artifactVersionDirPath.toString());
    } else {
      Path artifactsDirPath = artifactVersionDirPath.getParent();
      dfso.mkdir(artifactsDirPath, artifactPermission);
      setOwnershipAndPermissions(artifactsDirPath, artifactPermission, username, group, dfso);
      dfso.mkdir(artifactVersionDirPath, artifactPermission);
      setOwnershipAndPermissions(artifactVersionDirPath, artifactPermission, username, group, dfso);
    }
  
    // copy model files to artifact directory
    // -- per model version file
    Stack<Path> dirs = new Stack<>();
    for (FileStatus modelFile :
      dfso.listStatus(modelVersionPath)) {
      Path srcModelFilePath = new Path(String.format("%s/%s", modelVersionPath, modelFile.getPath().getName()));
      Path destModelFilePath = new Path(String.format("%s/%s", artifactVersionDirPath.toString(),
        modelFile.getPath().getName()));
      boolean isDirectory = modelFile.isDirectory();
      if (modelFile.getPath().getName().equals("Artifacts") && isDirectory) {
        continue; // ignore Artifacts folder
      }
    
      // copy file/dir into artifact folder
      if (dryRun) {
        LOGGER.info("Copying model file to artifact directory: " + srcModelFilePath.toString() + " -> " +
          destModelFilePath.toString());
      } else {
        dfso.copyInHdfs(srcModelFilePath, destModelFilePath);
        setOwnershipAndPermissions(destModelFilePath, artifactPermission, username, group, dfso);
      }
    
      // add directory to stack for later permissions/ownership update of children items
      if (isDirectory) {
        dirs.push(destModelFilePath);
      }
    }
    // update permissions and ownership recursively
    if (!dryRun) {
      while (!dirs.isEmpty()) {
        Path dirPath = dirs.pop();
        for (FileStatus modelFile : dfso.listStatus(dirPath)) {
          Path modelFilePath = modelFile.getPath();
          setOwnershipAndPermissions(modelFilePath, artifactPermission, username, group, dfso);
          if (modelFile.isDirectory()) {
            dirs.push(modelFilePath);
          }
        }
      }
    }
  }
  
  private void createArtifact(Path artifactVersionDir, Path artifactPath, FsPermission artifactPermission,
      String username, String group, DistributedFileSystemOps dfso)
      throws IllegalAccessException, SQLException, InstantiationException, IOException {
    
    // create local dir
    String localDir = expatVariablesFacade.findById("staging_dir").getValue() + File.separator +
      DigestUtils.sha256Hex(artifactVersionDir.toString()) + File.separator + "artifact";
    File zipDir = new File(localDir);
    if (dryRun) {
      LOGGER.info("Create local directory: " + localDir);
    } else {
      if (zipDir.exists()) {
        FileUtils.deleteDirectory(zipDir);
      }
      if (!zipDir.mkdirs()) {
        throw new IOException("Local directory could not be created: " + localDir);
      }
    }
    
    // copy artifact version directory to local
    if (dryRun) {
      LOGGER.info("Copy artifact version folder to local: " + artifactVersionDir.toString());
    } else {
      dfso.copyToLocal(artifactVersionDir.toString(), localDir);
    }
    
    // zip artifact version directory
    String artifactDirLocalPath = localDir + File.separator + artifactVersionDir.getName();
    String artifactFileLocalPath = zipDir.getParent() + File.separator + artifactPath.getName();
    if (dryRun) {
      LOGGER.info("Create artifact zip file at: " + artifactFileLocalPath);
    } else {
      zipDirectory(artifactDirLocalPath, artifactFileLocalPath);
    }
    
    // copy artifact file to hdfs
    if (dryRun) {
      LOGGER.info("Copy artifact file to HDFS at: " + artifactPath.toString());
    } else {
      dfso.copyFromLocal(true, new Path(artifactFileLocalPath), artifactPath);
      setOwnershipAndPermissions(artifactPath, artifactPermission, username, group, dfso);
    }
  
    LOGGER.info("New artifact created at:" + artifactPath.toString());
  }
  
  private void setOwnershipAndPermissions(Path filePath, FsPermission permissions, String username, String group,
    DistributedFileSystemOps dfso) throws IOException {
    dfso.setOwner(filePath, username, group);
    if (permissions != null)
      dfso.setPermission(filePath, permissions);
  }
  
  private String getHdfsUserName(String projectName, String username) {
    return projectName + HdfsUsersController.USER_NAME_DELIMITER + username;
  }
  
  private void zipDirectory(String dirPath, String zipFilePath) {
    try {
      FileOutputStream fos = new FileOutputStream(zipFilePath);
      ZipOutputStream zos = new ZipOutputStream(fos);
      
      List<String> filePaths = Files.walk(Paths.get(dirPath).getParent(), Integer.MAX_VALUE)
        .filter(file -> !Files.isDirectory(file) && !file.toString().endsWith(".crc"))
        .map(file -> file.getParent().toString() + File.separator + file.getFileName().toString())
        .collect(Collectors.toList());
  
      File dirFile = new File(dirPath);
      String parentAbsolutePath = dirFile.getParentFile().getAbsolutePath();
      for(String filePath : filePaths){
        ZipEntry ze = new ZipEntry(filePath.substring(parentAbsolutePath.length() + 1));
        zos.putNextEntry(ze);
        FileInputStream fis = new FileInputStream(filePath);
        byte[] buffer = new byte[1024];
        int len;
        while ((len = fis.read(buffer)) > 0) {
          zos.write(buffer, 0, len);
        }
        zos.closeEntry();
        fis.close();
      }
      zos.close();
      fos.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  private void setup() throws SQLException, ConfigurationException {
    Configuration conf = ConfigurationBuilder.getConfiguration();
    dryRun = conf.getBoolean(ExpatConf.DRY_RUN);
    hopsUser = conf.getString(ExpatConf.HOPS_CLIENT_USER);
    if (hopsUser == null) {
      throw new ConfigurationException(ExpatConf.HOPS_CLIENT_USER + " cannot be null");
    }
    connection = DbConnectionFactory.getConnection();
    expatVariablesFacade = new ExpatVariablesFacade(ExpatVariables.class, connection);
    expatUserFacade = new ExpatUserFacade();
  }
  
  private void closeConnections(PreparedStatement... stmts) {
    try {
      for (PreparedStatement stmt : stmts) {
        if (stmt != null) {
          stmt.close();
        }
      }
    } catch(SQLException ex) {
      //do nothing
    }
  }
}
