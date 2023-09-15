/**
 * This file is part of Expat
 * Copyright (C) 2023, Hopsworks AB. All rights reserved
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

package io.hops.hopsworks.expat.migrations.featurestore.metadata;

import io.hops.hopsworks.common.hdfs.DistributedFileSystemOps;
import io.hops.hopsworks.common.provenance.core.ProvXAttrs;
import io.hops.hopsworks.common.provenance.core.Provenance;
import io.hops.hopsworks.common.provenance.core.dto.ProvCoreDTO;
import io.hops.hopsworks.common.provenance.core.dto.ProvTypeDTO;
import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import io.hops.hopsworks.expat.db.DbConnectionFactory;
import io.hops.hopsworks.expat.migrations.MigrateStep;
import io.hops.hopsworks.expat.migrations.MigrationException;
import io.hops.hopsworks.expat.migrations.RollbackException;
import io.hops.hopsworks.expat.migrations.projects.util.HopsClient;
import io.hops.hopsworks.persistence.entity.hdfs.inode.Inode;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.hadoop.fs.Path;
import org.eclipse.persistence.jaxb.JAXBContextFactory;
import org.eclipse.persistence.jaxb.MarshallerProperties;
import org.eclipse.persistence.oxm.MediaType;
import org.opensearch.common.CheckedConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import java.io.IOException;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class DisableEpipeMigration implements MigrateStep {
  private final static Logger LOGGER = LoggerFactory.getLogger(DisableEpipeMigration.class);
  
  private final static String GET_PROJECTS = "SELECT id, projectname FROM project";
  private final static int GET_PROJECTS_S_ID = 1;
  private final static int GET_PROJECTS_S_NAME = 2;
  private final static String GET_DATASETS = "SELECT inode_name FROM dataset WHERE projectId=?";
  private final static int GET_DATASETS_S_NAME = 1;
  private final static int GET_DATASETS_W_PROJ_ID = 1;
  private final static String GET_PROJECT_INODE = "SELECT id FROM hops.hdfs_inodes WHERE parent_id=? AND name=?";
  private final static int GET_PROJECT_INODE_S_ID = 1;
  private final static int GET_PROJECT_INODE_W_PARENT_ID = 1;
  private final static int GET_PROJECT_INODE_W_NAME = 2;
  private final static String UPDATE_VARIABLES = "UPDATE variables SET value=? WHERE id=?";
  private final static int UPDATE_VARIABLES_W_VALUE = 1;
  private final static int UPDATE_VARIABLES_W_ID = 2;
  
  protected Connection connection = null;
  private String hopsUser;
  boolean dryrun = false;
  DistributedFileSystemOps dfso = null;
  
  private JAXBContext context;
  Marshaller marshaller;
  
  private void setup()
    throws ConfigurationException, SQLException, JAXBException {
    connection = DbConnectionFactory.getConnection();
    Configuration conf = ConfigurationBuilder.getConfiguration();
    hopsUser = conf.getString(ExpatConf.HOPS_CLIENT_USER);
    if (hopsUser == null) {
      throw new ConfigurationException(ExpatConf.HOPS_CLIENT_USER + " cannot be null");
    }
    dryrun = conf.getBoolean(ExpatConf.DRY_RUN);
    dfso = HopsClient.getDFSO(hopsUser);
    
    Map<String, Object> properties = new HashMap<>();
    properties.put(MarshallerProperties.JSON_INCLUDE_ROOT, false);
    properties.put(MarshallerProperties.MEDIA_TYPE, MediaType.APPLICATION_JSON);
    context = JAXBContextFactory.
      createContext(new Class[] {
        ProvCoreDTO.class,
        ProvTypeDTO.class,
      }, properties);
    marshaller = context.createMarshaller();
  }
  
  private void close() throws SQLException {
    if(connection != null) {
      connection.close();
    }
  }
  
  @Override
  public void migrate() throws MigrationException {
    LOGGER.info("epipe disable migration");
    try {
      setup();
      connection.setAutoCommit(false);
      updateVariable("DISABLED");
      traverseElements(migrateProject(), migrateDataset(), dryrun);
      connection.setAutoCommit(true);
    } catch (Throwable e) {
      throw new MigrationException("error", e);
    } finally {
      try {
        close();
      } catch (SQLException e) {
        LOGGER.info("problems closing sql connection", e);
      }
    }
  }
  
  @Override
  public void rollback() throws RollbackException {
    LOGGER.info("epipe disable tag rollback");
    try {
      setup();
      connection.setAutoCommit(false);
      updateVariable("FULL");
      traverseElements(rollbackProject(), rollbackDataset(), dryrun);
      connection.setAutoCommit(true);
    } catch (Throwable e) {
      throw new RollbackException("error", e);
    } finally {
      try {
        close();
      } catch (SQLException e) {
        LOGGER.info("problems closing sql connection", e);
      }
    }
  }
  
  private static class ProcessState {
    Integer projectId;
    String projectName;
    Long projectInodeId;
    String datasetName;
  }
  private void traverseElements(CheckedConsumer<ProcessState, Exception> projectAction,
                                CheckedConsumer<ProcessState, Exception> datasetAction,
                                boolean dryRun)
    throws Exception {
    try(PreparedStatement rootProjInodeStmt = connection.prepareStatement(GET_PROJECT_INODE)) {
      rootProjInodeStmt.setLong(GET_PROJECT_INODE_W_PARENT_ID, 1L);
      rootProjInodeStmt.setString(GET_PROJECT_INODE_W_NAME, "Projects");
      ResultSet rootProjInodeSet = rootProjInodeStmt.executeQuery();
      if(rootProjInodeSet.next()) {
        Long rootProjectInodeId = rootProjInodeSet.getLong(GET_PROJECT_INODE_S_ID);
        try (PreparedStatement projStmt = connection.prepareStatement(GET_PROJECTS)) {
          ResultSet projResultSet = projStmt.executeQuery();
          while (projResultSet.next()) {
            ProcessState state = new ProcessState();
            state.projectId = projResultSet.getInt(GET_PROJECTS_S_ID);
            state.projectName = projResultSet.getString(GET_PROJECTS_S_NAME);
            LOGGER.info("project:{}", state.projectName);
            try (PreparedStatement projInodeStmt = connection.prepareStatement(GET_PROJECT_INODE)) {
              projInodeStmt.setLong(GET_PROJECT_INODE_W_PARENT_ID, rootProjectInodeId);
              projInodeStmt.setString(GET_PROJECT_INODE_W_NAME, state.projectName);
              ResultSet projInodeResultSet = projInodeStmt.executeQuery();
              if (projInodeResultSet.next()) {
                state.projectInodeId = projInodeResultSet.getLong(GET_PROJECT_INODE_S_ID);
                LOGGER.info("project inode:{}", state.projectInodeId);
                if (!dryRun) {
                  projectAction.accept(state);
                }
                try (PreparedStatement datasetStmt = connection.prepareStatement(GET_DATASETS)) {
                  datasetStmt.setInt(GET_DATASETS_W_PROJ_ID, state.projectId);
                  ResultSet datasetResultSet = datasetStmt.executeQuery();
                  while (datasetResultSet.next()) {
                    state.datasetName = datasetResultSet.getString(GET_DATASETS_S_NAME);
                    LOGGER.info("dataset:{}", state.datasetName);
                    if (!dryRun) {
                      datasetAction.accept(state);
                    }
                  }
                  datasetResultSet.close();
                }
              }
              projInodeResultSet.close();
            }
          }
          projResultSet.close();
          connection.commit();
        }
      }
      rootProjInodeSet.close();
    }
  }
  
  private void updateVariable(String value) throws SQLException {
    try (PreparedStatement stmt = connection.prepareStatement(UPDATE_VARIABLES)) {
      stmt.setString(UPDATE_VARIABLES_W_VALUE, value);
      stmt.setString(UPDATE_VARIABLES_W_ID, "provenance_type");
      stmt.executeUpdate();
    }
  }
  
  private String projectPath(String projectName) {
    return "/Projects/" + projectName;
  }
  
  private CheckedConsumer<ProcessState, Exception> migrateProject() {
    return state -> {
      Path path = new Path(projectPath(state.projectName));
      setXAttr(path, new ProvCoreDTO(Provenance.Type.DISABLED.dto, null));
    };
  }
  
  private CheckedConsumer<ProcessState, Exception> rollbackProject() {
    return state -> {
      Path path = new Path(projectPath(state.projectName));
      setXAttr(path, new ProvCoreDTO(Provenance.Type.FULL.dto, null));
    };
  }
  
  private String datasetPath(String projectName, String datasetName) {
    if(datasetName.endsWith(".db")) {
      return "/apps/hive/warehouse/" + datasetName;
    } else {
      return projectPath(projectName) + "/" + datasetName;
    }
  }
  
  private CheckedConsumer<ProcessState, Exception> migrateDataset() {
    return state -> {
      Path path = new Path(datasetPath(state.projectName, state.datasetName));
      dfso.setMetaStatus(path, Inode.MetaStatus.DISABLED);
      setXAttr(path, new ProvCoreDTO(Provenance.Type.DISABLED.dto, state.projectInodeId));
    };
  }
  
  private CheckedConsumer<ProcessState, Exception> rollbackDataset() {
    return state -> {
      Path path = new Path(datasetPath(state.projectName, state.datasetName));
      if (state.datasetName.equals(state.projectName + "_Training_Datasets") ||
        state.datasetName.equals(state.projectName.toLowerCase() + "_featurestore.db")) {
        dfso.setMetaStatus(path, Inode.MetaStatus.FULL_PROV_ENABLED);
        setXAttr(path, new ProvCoreDTO(Provenance.Type.FULL.dto, state.projectInodeId));
      } else if (state.datasetName.equals(state.projectName.toLowerCase() + ".db")) {
        dfso.setMetaStatus(path, Inode.MetaStatus.META_ENABLED);
        setXAttr(path, new ProvCoreDTO(Provenance.Type.META.dto, state.projectInodeId));
      } else {
        switch (state.datasetName) {
          case "Logs":
          case "Resources":
          case "Statistics":
          case "Docker": {
            dfso.setMetaStatus(path, Inode.MetaStatus.DISABLED);
            setXAttr(path, new ProvCoreDTO(Provenance.Type.DISABLED.dto, state.projectInodeId));
          }
            break;
          case "Models":
          case "Experiments": {
            dfso.setMetaStatus(path, Inode.MetaStatus.FULL_PROV_ENABLED);
            setXAttr(path, new ProvCoreDTO(Provenance.Type.FULL.dto, state.projectInodeId));
          }
            break;
          case "Jupyter":
          case "DataValidation":
          case "Airflow":
          default:
            dfso.setMetaStatus(path, Inode.MetaStatus.META_ENABLED);
            setXAttr(path, new ProvCoreDTO(Provenance.Type.META.dto, state.projectInodeId));
        }
      }
    };
  }
  
  private void setXAttr(Path path, ProvCoreDTO xattr) throws JAXBException, IOException {
    String provType = marshal(xattr);
    dfso.setXAttr(path, "provenance." + ProvXAttrs.PROV_XATTR_CORE_VAL, provType.getBytes());
  }
  
  public <V> String marshal(V obj) throws JAXBException {
    StringWriter sw = new StringWriter();
    marshaller.marshal(obj, sw);
    return sw.toString();
  }
}
