/**
 * This file is part of Expat
 * Copyright (C) 2022, Logical Clocks AB. All rights reserved
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
import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import io.hops.hopsworks.expat.db.DbConnectionFactory;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.sql.Types;
import java.util.HashSet;
import java.util.List;
import java.util.Stack;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class PythonArtifactMigration implements MigrateStep {
  private static final Logger LOGGER = LoggerFactory.getLogger(PythonArtifactMigration.class);
  
  protected Connection connection;
  private boolean dryRun;
  private String hopsUser;
  private ExpatVariablesFacade expatVariablesFacade;
  
  private final static String GET_PROJECTS = "SELECT id, projectname FROM project";
  private final static String GET_SERVINGS = "SELECT id, model_path, model_version, artifact_version, model_server " +
    "FROM serving WHERE project_id = ?";
  private final static String GET_SERVINGS_WITH_PRED = "SELECT id, model_name, model_version, artifact_version, " +
    "model_server, predictor FROM serving WHERE project_id = ?";
  
  private final static String UPDATE_SERVING = "UPDATE serving SET model_path = ?, artifact_version = ? WHERE id = ?";
  private final static String UPDATE_SERVING_WITH_PRED = "UPDATE serving SET model_path = ?, artifact_version = ?, " +
    "predictor = ? WHERE id = ?";
  
  private final static String EXISTS_SERVING_PREDICTOR_COLUMN = "SHOW COLUMNS FROM `serving` LIKE 'predictor'";
  
  private final static String MODELS_PATH = "/Projects/%s/Models";
  private final static String MODEL_PATH = MODELS_PATH + "/%s";
  private final static String MODEL_VERSION_PATH = MODEL_PATH + "/%s";
  private final static String ARTIFACTS_PATH = MODEL_VERSION_PATH + "/Artifacts";
  private final static String ARTIFACT_VERSION_PATH = ARTIFACTS_PATH + "/%s";
  private final static String ARTIFACT_NAME = "%s_%s_%s.zip";
  private final static String PREDICTOR_PREFIX = "predictor-";
  private final static String NEW_PREDICTOR_NAME = "%s_%s";
  
  @Override
  public void migrate() throws MigrationException {
    LOGGER.info("Starting python artifacts migration");
    
    try {
      setup();
    } catch (SQLException | ConfigurationException ex) {
      String errorMsg = "Could not initialize database connection";
      LOGGER.error(errorMsg);
      throw new MigrationException(errorMsg, ex);
    }
    
    boolean isKubeInstalled;
    try {
      // check kubernetes is installed
      ExpatVariables kubernetesInstalled = expatVariablesFacade.findById("kubernetes_installed");
      isKubeInstalled = Boolean.parseBoolean(kubernetesInstalled.getValue());
    } catch (IllegalAccessException | SQLException | InstantiationException ex) {
      String errorMsg = "Could not migrate python artifacts";
      LOGGER.error(errorMsg);
      throw new MigrationException(errorMsg, ex);
    }
  
    PreparedStatement getProjectsStmt = null;
    PreparedStatement getServingsStmt = null;
    PreparedStatement updateServingWithPredStmt = null;
    DistributedFileSystemOps dfso = null;
    try {
      connection.setAutoCommit(false);
      dfso = HopsClient.getDFSO(hopsUser);
  
      boolean updateServings = false;
      updateServingWithPredStmt = connection.prepareStatement(UPDATE_SERVING_WITH_PRED);
      
      // -- per project
      getProjectsStmt = connection.prepareStatement(GET_PROJECTS);
      ResultSet projectsResultSet = getProjectsStmt.executeQuery();
      while(projectsResultSet.next()) {
        // parse project query results
        int projectId = projectsResultSet.getInt(1);
        String projectName = projectsResultSet.getString(2);
        
        HashSet<String> keepModelArtifacts = new HashSet<>();
        
        // -- per serving
        getServingsStmt = connection.prepareStatement(GET_SERVINGS);
        getServingsStmt.setInt(1, projectId);
        ResultSet servingsResultSet = getServingsStmt.executeQuery();
        while(servingsResultSet.next()) {
          // parse serving query result
          int servingId = servingsResultSet.getInt(1);
          String modelPath = servingsResultSet.getString(2);
          int modelVersion = servingsResultSet.getInt(3);
          int artifactVersion = servingsResultSet.getInt(4);
          int modelServer = servingsResultSet.getInt(5);
          String newModelPath = extractNewModelPath(projectName, modelPath);
          String predictor = extractPredictorFilename(modelPath);
          
          if (isKubeInstalled) {
            // if kubernetes is installed
            if (artifactVersion == 0) {
              // and artifact is model-only
              String modelName = extractModelName(projectName, modelPath);
              if (modelServer == 0) {
                // if tensorflow serving and artifact version 0, add this model to be ignored when deleting artifacts
                // with version 0
                keepModelArtifacts.add(modelName + "/" + modelVersion);
                continue;
              }
              if (modelServer == 1) {
                // if flask server
                if (!modelPath.endsWith(".py")) {
                  // if model path does not point to a script, it's been already updated
                  keepModelArtifacts.add(modelName + "/" + modelVersion);
                  continue; // ignore serving
                }
                // migrate artifact
                int newArtifactVersion = migratePythonArtifact(projectName, modelName, modelVersion, predictor, dfso);
                // update serving
                updateServing(servingId, newModelPath, newArtifactVersion, predictor, updateServingWithPredStmt);
                updateServings = true;
              }
            }
          } else {
            // if kubernetes is not installed, we don't create artifacts but we have to update python servings
            if (modelServer == 1) {
              // if flask server
              if (modelPath.endsWith(".py")) {
                // if model path points to a script, the serving hasn't been updated yet.
                // In non-kubernetes installations, predictor contains the full path to the script
                updateServing(servingId, newModelPath, null, modelPath, updateServingWithPredStmt);
                updateServings = true;
              }
            }
          }
        } // -- end -- per serving
        
        if (isKubeInstalled) {
          // delete unused version 0 artifacts
          deletePythonArtifacts(projectName, keepModelArtifacts, dfso);
        }
      } // -- end -- per project
      
      // update servings
      if (dryRun) {
        LOGGER.info(updateServingWithPredStmt.toString());
      } else {
        if (updateServings) { updateServingWithPredStmt.executeBatch(); }
      }
      
      connection.commit();
      connection.setAutoCommit(true);
    } catch(IllegalAccessException | InstantiationException | IllegalStateException | IOException | SQLException ex) {
      String errorMsg = "Could not migrate python artifact";
      LOGGER.error(errorMsg);
      throw new MigrationException(errorMsg, ex);
    } finally {
      closeConnections(getProjectsStmt, getServingsStmt, updateServingWithPredStmt);
      if (dfso != null) {
        dfso.close();
      }
    }
    LOGGER.info("Finished python artifacts migration");
  }
  
  @Override
  public void rollback() throws RollbackException {
    LOGGER.info("Starting python artifacts rollback");
    
    try {
      setup();
    } catch (SQLException | ConfigurationException ex) {
      String errorMsg = "Could not initialize database connection";
      LOGGER.error(errorMsg);
      throw new RollbackException(errorMsg, ex);
    }
  
    boolean isKubeInstalled;
    try {
      // check kubernetes is installed
      ExpatVariables kubernetesInstalled = expatVariablesFacade.findById("kubernetes_installed");
      isKubeInstalled = Boolean.parseBoolean(kubernetesInstalled.getValue());
    } catch (IllegalAccessException | SQLException | InstantiationException ex) {
      String errorMsg = "Could not migrate python artifact";
      LOGGER.error(errorMsg);
      throw new RollbackException(errorMsg, ex);
    }
  
    PreparedStatement getProjectsStmt = null;
    PreparedStatement getServingsWithPredStmt = null;
    PreparedStatement updateServingStmt = null;
    PreparedStatement existsServingPredictorStmt = null;
    DistributedFileSystemOps dfso = null;
    try {
      connection.setAutoCommit(false);
      dfso = HopsClient.getDFSO(hopsUser);
  
      existsServingPredictorStmt = connection.prepareStatement(EXISTS_SERVING_PREDICTOR_COLUMN);
      ResultSet existsResultSet = existsServingPredictorStmt.executeQuery();
      if (!existsResultSet.next()){
        // if predictor column does not exist, rollback was already done
        // do nothing
      } else {
        boolean updateServings = false;
        updateServingStmt = connection.prepareStatement(UPDATE_SERVING);
        
        // -- per project
        getProjectsStmt = connection.prepareStatement(GET_PROJECTS);
        ResultSet projectsResultSet = getProjectsStmt.executeQuery();
        while(projectsResultSet.next()) {
          // parse project query results
          int projectId = projectsResultSet.getInt(1);
          String projectName = projectsResultSet.getString(2);
    
          HashSet<String> keepModelArtifacts = new HashSet<>();
          HashSet<String> createModelArtifactV0 = new HashSet<>();
    
          // -- per serving
          getServingsWithPredStmt = connection.prepareStatement(GET_SERVINGS_WITH_PRED);
          getServingsWithPredStmt.setInt(1, projectId);
          ResultSet servingsResultSet = getServingsWithPredStmt.executeQuery();
          while (servingsResultSet.next()) {
            // parse serving query result
            int servingId = servingsResultSet.getInt(1);
            String modelName = servingsResultSet.getString(2);
            int modelVersion = servingsResultSet.getInt(3);
            int artifactVersion = servingsResultSet.getInt(4);
            int modelServer = servingsResultSet.getInt(5);
            String predictor = servingsResultSet.getString(6);
            String newPredictor = String.format(NEW_PREDICTOR_NAME, artifactVersion, predictor);
            
            if (modelServer == 1) {
              // if flask
              if (artifactVersion > 0) {
                // and artifact version > 0
                if (isKubeInstalled) {
                  // Keep track of model name for later creation of artifact version 0
                  createModelArtifactV0.add(modelName + "/" + modelVersion);
  
                  // Copy predictor script to model version folder if it doesn't already exists.
                  Path modelVersionPath = new Path(String.format(MODEL_VERSION_PATH, projectName, modelName,
                    modelVersion));
                  FileStatus fileStatus = dfso.getFileStatus(modelVersionPath);
                  FsPermission permission = fileStatus.getPermission();
                  String username = fileStatus.getOwner();
                  String group = fileStatus.getGroup();
                  Path artifactVersionPath = new Path(String.format(ARTIFACT_VERSION_PATH, projectName, modelName,
                    modelVersion, artifactVersion));
                  
                  copyPredictorFileToModelVersionFolder(modelVersionPath, artifactVersionPath, predictor, newPredictor,
                    permission, username, group, dfso);
  
                  // Delete artifact version
                  if (dryRun) {
                    LOGGER.info("Delete artifact version directory: " + artifactVersionPath.toString());
                  } else {
                    dfso.rm(artifactVersionPath, true);
                  }
                }
                
                // Update serving
                // - modelPath -> predictor script in model version folder
                // - artifactVersion -> 0 or null (no k8s)
                // - predictor -> will be removed
                String scriptPath = String.format(MODEL_VERSION_PATH + "/%s", projectName, modelName, modelVersion,
                  newPredictor);
                updateServing(servingId, scriptPath, isKubeInstalled ? 0 : null, null, updateServingStmt);
                updateServings = true;
              } else {
                // and artifact version = 0
                LOGGER.info(String.format("Migration of MODEL-ONLY artifact ignored for model %s and version " +
                  "%s in project %s", modelName, modelVersion, projectName));
              }
            } else if(modelServer == 0) {
              if (isKubeInstalled) {
                // if tensorflow serving, keep track of model name/version to avoid removing its artifacts
                keepModelArtifacts.add(modelName + "/" + modelVersion);
              }
            }
          } // -- end -- per serving
  
          if (isKubeInstalled) {
            // Delete unused artifacts and create artifact version 0 for python servings
            deleteNewAndCreateV0Artifacts(projectName, keepModelArtifacts, createModelArtifactV0, dfso);
          }
        } // -- end -- per project
  
        // update servings
        if (dryRun) {
          LOGGER.info(updateServingStmt.toString());
        } else {
          if (updateServings) { updateServingStmt.executeBatch(); }
        }
      }
      
      connection.commit();
      connection.setAutoCommit(true);
    } catch(IllegalStateException | SQLException | IOException | IllegalAccessException | InstantiationException ex) {
      String errorMsg = "Could not rollback python artifact";
      LOGGER.error(errorMsg);
      throw new RollbackException(errorMsg, ex);
    } finally {
      closeConnections(getProjectsStmt, getServingsWithPredStmt, updateServingStmt, existsServingPredictorStmt);
      if (dfso != null) {
        dfso.close();
      }
    }
    LOGGER.info("Finished python artifacts migration");
  }
  
  private int migratePythonArtifact(String projectName, String modelName, int modelVersion, String predictor,
    DistributedFileSystemOps dfso) throws IOException, IllegalAccessException, SQLException, InstantiationException {
    LOGGER.info(String.format("Migration of artifact for model %s and version %s with predictor %s in project %s",
      modelName, modelVersion, predictor, projectName));
    
    // -- per artifact version
    int artifactVersion = 1;
    Path artifactPath = new Path(String.format(ARTIFACT_VERSION_PATH, projectName, modelName, modelVersion,
      artifactVersion));
    while(dfso.exists(artifactPath)) {
      // if artifact exists, check predictor script
      String scriptFile = String.format("%s/%s", artifactPath.toString(), PREDICTOR_PREFIX + predictor);
      Path scriptFilepath = new Path(scriptFile);
      if (dfso.exists(scriptFilepath)) {
        // if predictor script already exists, return artifact id
        return artifactVersion;
      }
      // otherwise, increase artifact version
      artifactVersion += 1;
      artifactPath = new Path(String.format(ARTIFACT_VERSION_PATH, projectName, modelName, modelVersion,
        artifactVersion));
    }
    
    // create artifacts version 1 directory
    Path modelVersionPath = new Path(String.format(MODEL_VERSION_PATH, projectName, modelName, modelVersion));
    FileStatus fileStatus = dfso.getFileStatus(modelVersionPath);
    FsPermission permission = fileStatus.getPermission();
    String username = fileStatus.getOwner();
    String group = fileStatus.getGroup();
    
    // copy files from model version to artifact version 1
    copyFilesToArtifactFolder(modelVersionPath, artifactPath, permission, username, group, dfso);
    
    // rename predictor script with suffix predictor-
    copyPredictorFileToArtifactFolder(modelVersionPath, artifactPath, predictor, permission, username, group, dfso);
    
    // create artifact
    String artifactFile = String.format("%s/" + ARTIFACT_NAME, artifactPath.toString(), modelName, modelVersion,
      artifactVersion);
    Path artifactFilepath = new Path(artifactFile);
    createArtifact(artifactPath, artifactFilepath, permission, username, group, dfso);

    return artifactVersion;
  }
  
  private void updateServing(int servingId, String modelPath, Integer artifactVersion, String predictor,
      PreparedStatement updateServingStmt) throws SQLException {
    updateServingStmt.setString(1, modelPath);
    if (artifactVersion != null) {
      updateServingStmt.setInt(2, artifactVersion);
    } else {
      updateServingStmt.setNull(2, Types.INTEGER);
    }
    if (predictor != null) {
      updateServingStmt.setString(3, predictor);
      updateServingStmt.setInt(4, servingId);
    } else {
      updateServingStmt.setInt(3, servingId);
    }
    updateServingStmt.addBatch();
  }
  
  private void copyFilesToArtifactFolder(Path modelVersionPath, Path artifactVersionDirPath,
    FsPermission artifactPermission, String username, String group, DistributedFileSystemOps dfso) throws IOException {
    
    // create artifact directory
    if (dryRun) {
      LOGGER.info("Create artifacts directory if it doesn't exist: "+ artifactVersionDirPath.toString());
    } else {
      Path artifactsDirPath = artifactVersionDirPath.getParent();
      if (!dfso.exists(artifactsDirPath)) {
        if (dryRun) {
          LOGGER.info("Create artifacts directory: " + artifactsDirPath.toString());
        } else {
          dfso.mkdir(artifactsDirPath, artifactPermission);
          setOwnershipAndPermissions(artifactsDirPath, artifactPermission, username, group, dfso);
        }
      }
      if (dryRun) {
        LOGGER.info("Create artifact version directory: " + artifactVersionDirPath.toString());
      } else {
        dfso.mkdir(artifactVersionDirPath, artifactPermission);
        setOwnershipAndPermissions(artifactVersionDirPath, artifactPermission, username, group, dfso);
      }
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
        LOGGER.info("Copying artifact content file to artifact directory: " + srcModelFilePath.toString() + " -> " +
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
  
  private void copyFileIfNotExists(Path srcFilePath, Path destFilePath, FsPermission permissions, String username,
    String group, DistributedFileSystemOps dfso) throws IOException {
    if (dfso.exists(destFilePath)) {
      return; // predictor script already exists in dest folder
    }
  
    if (dryRun) {
      LOGGER.info("Copying file to directory: " + srcFilePath.toString() + " -> " +
        destFilePath.toString());
    } else {
      dfso.copyInHdfs(srcFilePath, destFilePath);
      setOwnershipAndPermissions(destFilePath, permissions, username, group, dfso);
    }
  }
  
  private void copyPredictorFileToArtifactFolder(Path modelVersionPath, Path artifactVersionDirPath,
    String predictor, FsPermission permissions, String username, String group, DistributedFileSystemOps dfso)
    throws IOException {
    Path srcPredictorFilepath = new Path(String.format("%s/%s", modelVersionPath.toString(), predictor));
    Path destPredictorFilepath = new Path(String.format("%s/%s", artifactVersionDirPath.toString(),
      PREDICTOR_PREFIX + predictor));
    copyFileIfNotExists(srcPredictorFilepath, destPredictorFilepath, permissions, username, group, dfso);
  }
  
  private void copyPredictorFileToModelVersionFolder(Path modelVersionPath, Path artifactVersionDirPath,
    String srcPredictor, String destPredictor, FsPermission permissions, String username, String group,
    DistributedFileSystemOps dfso)
    throws IOException {
    Path srcPredictorFilepath = new Path(String.format("%s/%s", artifactVersionDirPath.toString(),
      PREDICTOR_PREFIX + srcPredictor));
    Path destPredictorFilepath = new Path(String.format("%s/%s", modelVersionPath.toString(), destPredictor));
    copyFileIfNotExists(srcPredictorFilepath, destPredictorFilepath, permissions, username, group, dfso);
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
  
  private String extractModelName(String projectName, String modelPath) {
    String prefix = "/Projects/" + projectName + "/Models/";
    if (modelPath.endsWith(".py")) {
      // if old model path, extract new path
      modelPath = extractNewModelPath(projectName, modelPath);
    }
    return modelPath.replace(prefix, "");
  }
  
  private String extractNewModelPath(String projectName, String modelPath) {
    if (!modelPath.endsWith(".py")) {
      // if not pointing to a script, it's already new model path
      return modelPath;
    }
    String prefix = "/Projects/" + projectName + "/Models/";
    int idx = modelPath.replace(prefix,"").indexOf("/");
    return modelPath.substring(0, prefix.length() + idx);
  }
  
  private String extractPredictorFilename(String modelPath) {
    int lastSlash = modelPath.lastIndexOf("/");
    int startName = (lastSlash > -1) ? lastSlash + 1 : 0;
    return modelPath.substring(startName);
  }
  
  private void deletePythonArtifacts(String projectName, HashSet<String> keepModelArtifacts,
    DistributedFileSystemOps dfso) throws IOException {
    Path modelsPath = new Path(String.format(MODELS_PATH, projectName));
    if(!dfso.exists(modelsPath)){
      LOGGER.info("Project " + projectName + " doesn't have models directory.");
      return;
    }
    
    LOGGER.info(String.format("Delete sklearn artifacts for project %s", projectName));
    
    // -- per model
    for (FileStatus modelDir : dfso.listStatus(new Path(String.format(MODELS_PATH, projectName)))) {
      if (!modelDir.isDirectory()) {
        continue; // ignore files
      }
      String modelName = modelDir.getPath().getName();
      
      // -- per model version
      Path modelPath = new Path(String.format(MODEL_PATH, projectName, modelName));
      for (FileStatus modelVersionDir : dfso.listStatus(modelPath)) {
        if (!modelVersionDir.isDirectory()) {
          continue; // ignore files
        }
        
        String modelVersion = modelVersionDir.getPath().getName();
        try {
          Integer.parseInt(modelVersion);
        } catch (NumberFormatException nfe) {
          continue; // ignore other than model version directories
        }
  
        if (keepModelArtifacts.contains(modelName + "/" + modelVersion)) {
          continue; // ignore artifacts of this model
        }
        
        if (isTensorflowModel(modelPath, dfso)) {
          continue; // ignore tensorflow models
        }
        
        // delete artifact version 0 folder
        Path artifactPath = new Path(String.format(ARTIFACT_VERSION_PATH, projectName, modelName, modelVersion, 0));
        if (dryRun) {
          LOGGER.info("Delete artifact version 0 directory: " + artifactPath.toString());
        } else {
          dfso.rm(artifactPath, true);
        }
      }
    }
  }
  
  private void deleteNewAndCreateV0Artifacts(String projectName, HashSet<String> ignoreModels,
    HashSet<String> createModelArtifactV0, DistributedFileSystemOps dfso)
    throws IOException, IllegalAccessException, SQLException, InstantiationException {
    Path modelsPath = new Path(String.format(MODELS_PATH, projectName));
    if(!dfso.exists(modelsPath)){
      LOGGER.info("Project " + projectName + " doesn't have models directory.");
      return;
    }
    
    LOGGER.info(String.format("Rollback of new artifacts for models in project %s", projectName));
    
    // -- per model
    for (FileStatus modelDir : dfso.listStatus(new Path(String.format(MODELS_PATH, projectName)))) {
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
  
        if (ignoreModels.contains(modelName + "/" + modelVersion)) {
          continue; // ignore artifacts of this model
        }
        
        // if .pb model file in model version folder -> continue
        Path modelVersionPath = new Path(String.format(MODEL_VERSION_PATH, projectName, modelName, modelVersion));
        if(isTensorflowModel(modelVersionPath, dfso)) {
          continue;
        }
        
        // -- per artifact version
        Path artifactsPath = new Path(String.format(ARTIFACTS_PATH, projectName, modelName, modelVersion));
        if (dfso.exists(artifactsPath)) {
          for (FileStatus artifactVersionDir : dfso.listStatus(artifactsPath)) {
            if (!artifactVersionDir.isDirectory()) {
              continue; // ignore files
            }
    
            String artifactVersionStr = artifactVersionDir.getPath().getName();
            Integer artifactVersion;
            try {
              artifactVersion = Integer.parseInt(artifactVersionStr);
            } catch (NumberFormatException nfe) {
              continue; // ignore other than artifact version directories
            }
    
            if (artifactVersion > 0) {
              // if artifact version > 0
              Path artifactVersionPath = new Path(String.format(ARTIFACT_VERSION_PATH, projectName, modelName,
                modelVersion, artifactVersion));
              // copy predictor script to model version folder
              Path predictorPath = findPredictor(artifactVersionPath, dfso);
              if (predictorPath != null){
                String srcPredictor = predictorPath.getName();
                String destPredictor = String.format(NEW_PREDICTOR_NAME, artifactVersion, srcPredictor);
                FileStatus fileStatus = dfso.getFileStatus(modelVersionPath);
                FsPermission permission = fileStatus.getPermission();
                String username = fileStatus.getOwner();
                String group = fileStatus.getGroup();
                copyPredictorFileToModelVersionFolder(modelVersionPath, artifactVersionPath, srcPredictor,
                  destPredictor, permission, username, group, dfso);
              }
              // remove artifact version
              if (dryRun) {
                LOGGER.info("Delete artifact version directory: " + artifactVersionPath.toString());
              } else {
                dfso.rm(artifactVersionPath, true);
              }
            }
          }
        }
        
        // if tracked model name and version, re-create artifact version 0
        if (createModelArtifactV0.contains(modelName + "/" + modelVersion)) {
          Path artifactV0Path = new Path(String.format(ARTIFACT_VERSION_PATH, projectName, modelName, modelVersion, 0));
          if (dfso.exists(artifactV0Path)) {
            if (dryRun) {
              LOGGER.info("Delete artifact version 0 directory: " + artifactV0Path.toString());
            } else {
              dfso.rm(artifactV0Path, true);
            }
          }
          // copy files to artifact version 0 folder
          FileStatus fileStatus = dfso.getFileStatus(modelVersionPath);
          FsPermission permission = fileStatus.getPermission();
          String username = fileStatus.getOwner();
          String group = fileStatus.getGroup();
          copyFilesToArtifactFolder(modelVersionPath, artifactV0Path, permission, username, group, dfso);
          // create artifact
          String artifactFile = String.format("%s/" + ARTIFACT_NAME, artifactV0Path.toString(), modelName, modelVersion,
            0);
          Path artifactFilepath = new Path(artifactFile);
          createArtifact(artifactV0Path, artifactFilepath, permission, username, group, dfso);
        }
      }
    }
  }
  
  private boolean isTensorflowModel(Path modelVersionPath, DistributedFileSystemOps dfso) throws IOException {
    for (FileStatus file : dfso.listStatus(modelVersionPath)) {
      if (file.isDirectory()) {
        continue; // ignore directories
      }
      if (file.getPath().getName().endsWith(".pb")) {
        return true;
      }
    }
    return false;
  }
  
  private Path findPredictor(Path path, DistributedFileSystemOps dfso) throws IOException {
    for (FileStatus file : dfso.listStatus(path)) {
      if (file.isDirectory()) {
        continue; // ignore directories
      }
      Path filepath = file.getPath();
      String name = filepath.getName();
      if (name.startsWith(PREDICTOR_PREFIX)) {
        return filepath;
      }
    }
    return null; // predictor not found
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
