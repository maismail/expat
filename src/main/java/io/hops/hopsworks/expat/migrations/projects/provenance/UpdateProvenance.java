/**
 * This file is part of Expat
 * Copyright (C) 2019, Logical Clocks AB. All rights reserved
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
package io.hops.hopsworks.expat.migrations.projects.provenance;

import io.hops.hopsworks.common.hdfs.DistributedFileSystemOps;
import io.hops.hopsworks.common.provenance.core.Provenance;
import io.hops.hopsworks.common.provenance.core.dto.ProvCoreDTO;
import io.hops.hopsworks.common.provenance.core.dto.ProvTypeDTO;
import io.hops.hopsworks.common.provenance.util.functional.CheckedConsumer;
import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import io.hops.hopsworks.expat.db.DbConnectionFactory;
import io.hops.hopsworks.expat.elastic.ElasticClient;
import io.hops.hopsworks.expat.migrations.MigrateStep;
import io.hops.hopsworks.expat.migrations.MigrationException;
import io.hops.hopsworks.expat.migrations.RollbackException;
import io.hops.hopsworks.expat.migrations.projects.util.XAttrException;
import io.hops.hopsworks.expat.migrations.projects.util.XAttrHelper;
import io.hops.hopsworks.expat.migrations.projects.util.HopsClient;
import io.hops.hopsworks.persistence.entity.hdfs.inode.Inode;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.http.HttpHost;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.eclipse.persistence.jaxb.JAXBContextFactory;
import org.eclipse.persistence.jaxb.MarshallerProperties;
import org.eclipse.persistence.oxm.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import java.io.IOException;
import java.io.StringWriter;
import java.security.GeneralSecurityException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class UpdateProvenance implements MigrateStep {
  private final static Logger LOGGER = LoggerFactory.getLogger(UpdateProvenance.class);
  
  private final static String GET_ALL_PROJECTS = "SELECT id, partition_id, inode_pid, inode_name FROM project";
  private final static int GET_ALL_PROJECTS_S_ID = 1;
  private final static int GET_ALL_PROJECTS_S_PARTITION_ID = 2;
  private final static int GET_ALL_PROJECTS_S_INODE_PID = 3;
  private final static int GET_ALL_PROJECTS_S_INODE_NAME = 4;
  private final static String GET_INODE =  "SELECT id, meta_enabled FROM hops.hdfs_inodes " +
    "WHERE partition_id=? && parent_id=? && name=?";
  private final static int GET_INODE_S_ID = 1;
  private final static int GET_INODE_S_META_ENABLED = 2;
  private final static int GET_INODE_W_PARTITION_ID = 1;
  private final static int GET_INODE_W_PARENT_ID = 2;
  private final static int GET_INODE_W_NAME = 3;
  private final static String GET_PROJECT_DATASETS = "SELECT inode_pid, inode_name, partition_id " +
    "FROM dataset WHERE projectId=?";
  private final static int GET_PROJECT_DATASETS_S_INODE_PID = 1;
  private final static int GET_PROJECT_DATASETS_S_INODE_NAME = 2;
  private final static int GET_PROJECT_DATASETS_S_PARTITION_ID = 3;
  private final static int GET_PROJECT_DATASETS_W_PROJECT_ID = 1;
  
  protected Connection connection;
  private CloseableHttpClient httpClient;
  private HttpHost elastic;
  private String elasticUser;
  private String elasticPass;
  private String hopsUser;
  
  private void setup()
    throws SQLException, ConfigurationException, GeneralSecurityException {
    connection = DbConnectionFactory.getConnection();
    Configuration conf = ConfigurationBuilder.getConfiguration();
    String elasticURI = conf.getString(ExpatConf.ELASTIC_URI);
  
    if (elasticURI == null) {
      throw new ConfigurationException(ExpatConf.ELASTIC_URI + " cannot be null");
    }
  
    elastic = HttpHost.create(elasticURI);
    elasticUser = conf.getString(ExpatConf.ELASTIC_USER_KEY);
    if (elasticUser == null) {
      throw new ConfigurationException(ExpatConf.ELASTIC_USER_KEY + " cannot be null");
    }
    elasticPass = conf.getString(ExpatConf.ELASTIC_PASS_KEY);
    if (elasticPass == null) {
      throw new ConfigurationException(ExpatConf.ELASTIC_PASS_KEY + " cannot be null");
    }
    hopsUser = conf.getString(ExpatConf.HOPS_CLIENT_USER);
    if (hopsUser == null) {
      throw new ConfigurationException(ExpatConf.HOPS_CLIENT_USER + " cannot be null");
    }
    httpClient = HttpClients
      .custom()
      .setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(
        CookieSpecs.IGNORE_COOKIES).build())
      .setSSLContext(new SSLContextBuilder().loadTrustMaterial((x509Certificates, s) -> true).build())
      .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
      .build();
  }
  
  private void close() throws SQLException, IOException {
    if(connection != null) {
      connection.close();
    }
    if(httpClient != null) {
      httpClient.close();
    }
  }
  
  @Override
  public void migrate() throws MigrationException {
    LOGGER.info("provenance migration");
    DistributedFileSystemOps dfso = null;
    try {
      setup();
      dfso = HopsClient.getDFSO(hopsUser);
      traverseElements(projectMigrate(dfso), datasetMigrate(dfso));
    } catch (IllegalStateException | SQLException | ConfigurationException | GeneralSecurityException | IOException e) {
      throw new MigrationException("error", e);
    } finally {
      if (dfso != null) {
        dfso.close();
      }
      try {
        close();
      } catch (SQLException | IOException e) {
        throw new MigrationException("error", e);
      }
    }
  }
  
  @Override
  public void rollback() throws RollbackException {
    LOGGER.info("provenance rollback");
    DistributedFileSystemOps dfso = null;
    try {
      setup();
      dfso = HopsClient.getDFSO(hopsUser);
      traverseElements(projectRollback(dfso), datasetRollback(dfso));
      ElasticClient.deleteAppProvenanceIndex(httpClient, elastic, elasticUser, elasticPass);
    } catch (IllegalStateException | SQLException | ConfigurationException | GeneralSecurityException | IOException e) {
      throw new RollbackException("error", e);
    } finally {
      if (dfso != null) {
        dfso.close();
      }
      try {
        close();
      } catch (SQLException | IOException e) {
        throw new RollbackException("error", e);
      }
    }
  }
  
  private <E extends Exception> void traverseElements(CheckedConsumer<ProjectParams, E> projectAction,
    CheckedConsumer<DatasetParams, E> datasetAction)
    throws E, SQLException, ConfigurationException, IOException, GeneralSecurityException {
    setup();
    
    PreparedStatement allProjectsStmt = null;
    PreparedStatement projectInodeStmt = null;
    PreparedStatement allProjectDatasetsStmt = null;
    PreparedStatement datasetInodeStmt = null;
    try {
      connection.setAutoCommit(false);
      //get all projects
      allProjectsStmt = connection.prepareStatement(GET_ALL_PROJECTS);
      ResultSet allProjectsResultSet = allProjectsStmt.executeQuery();
      
      while (allProjectsResultSet.next()) {
        //get project inode
        projectInodeStmt = getProjectInodeStmt(allProjectsResultSet);
        ResultSet projectInodeResultSet = projectInodeStmt.executeQuery();
        if(!projectInodeResultSet.next()) {
          throw new IllegalStateException("project inode not found");
        }
        
        ProjectParams projectParams = ProjectParams.instance(allProjectsResultSet, projectInodeResultSet);
        LOGGER.info("processing project:{}", projectParams.projectName);
        //get all project datasets
        allProjectDatasetsStmt = getProjectDatasetsStmt(allProjectsResultSet);
        ResultSet allProjectDatasetsResultSet = allProjectDatasetsStmt.executeQuery();
  
        while(allProjectDatasetsResultSet.next()) {
          //get dataset inode
          datasetInodeStmt = getDatasetInodeStmt(allProjectDatasetsResultSet);
          ResultSet datasetInodeResultSet = datasetInodeStmt.executeQuery();
          if(!datasetInodeResultSet.next()) {
            throw new IllegalStateException("dataset inode not found");
          }
          //update dataset meta status xattr and meta enabled column
          DatasetParams datasetParams = DatasetParams.instance(projectParams, allProjectDatasetsResultSet,
            datasetInodeResultSet);
          LOGGER.debug("processing dataset:{}", datasetParams.datasetName);
          datasetAction.accept(datasetParams);
          LOGGER.debug("processed dataset:{}", datasetParams.datasetName);
          datasetInodeStmt.close();
        }
        //update project meta status xattr
        
        projectAction.accept(projectParams);
        LOGGER.info("processed project:{}", projectParams.projectName);
        projectInodeStmt.close();
        allProjectDatasetsStmt.close();
      }
      allProjectsStmt.close();
      connection.commit();
      connection.setAutoCommit(true);
    } finally {
      if(allProjectsStmt != null) {
        allProjectsStmt.close();
      }
      if(projectInodeStmt != null) {
        projectInodeStmt.close();
      }
      if(allProjectDatasetsStmt != null) {
        allProjectDatasetsStmt.close();
      }
      if(datasetInodeStmt != null) {
        datasetInodeStmt.close();
      }
      close();
    }
  }
  
  private CheckedConsumer<ProjectParams, MigrationException> projectMigrate(DistributedFileSystemOps dfso) {
    return params -> {
      try {
        String projectPath = getProjectPath(params.projectName);
        if(!dfso.isDir(projectPath)) {
          LOGGER.warn("project with no directory:{}", projectPath);
          return;
        }
        JAXBContext jaxbContext = jaxbContext();
        ProvCoreDTO provCore = new ProvCoreDTO(Provenance.Type.MIN.dto, params.projectIId);
        byte[] bProvCore = jaxbParser(jaxbContext, provCore).getBytes();
    
        XAttrHelper.upsertProvXAttr(dfso, projectPath, "core", bProvCore);
      } catch (JAXBException | XAttrException e) {
        throw new MigrationException("error", e);
      }
    };
  }
  
  private CheckedConsumer<DatasetParams, MigrationException> datasetMigrate(DistributedFileSystemOps dfso) {
    return params -> {
      String datasetPath = getDatasetPath(params.projectIId, params.projectName, params.datasetPId, params.datasetName);
      try {
        if(!dfso.isDir(datasetPath)) {
          LOGGER.warn("dataset with no directory:{}", datasetPath);
          return;
        }
        JAXBContext jaxbContext = jaxbContext();
        ProvCoreDTO provCore;
        if (params.metaStatus == 0) {
          provCore = new ProvCoreDTO(Provenance.Type.DISABLED.dto, params.projectIId);
        } else if (params.metaStatus == 1) {
          provCore = new ProvCoreDTO(Provenance.Type.MIN.dto, params.projectIId);
          dfso.setMetaStatus(datasetPath, Inode.MetaStatus.MIN_PROV_ENABLED);
        } else if (params.metaStatus == 2) {
          provCore = new ProvCoreDTO(Provenance.Type.MIN.dto, params.projectIId);
        } else if (params.metaStatus == 3) {
          provCore = new ProvCoreDTO(Provenance.Type.FULL.dto, params.projectIId);
        } else {
          throw new IllegalStateException("unknown meta status:" + params.metaStatus);
        }
        byte[] bProvCore = jaxbParser(jaxbContext, provCore).getBytes();
        XAttrHelper.upsertProvXAttr(dfso, datasetPath, "core", bProvCore);
      } catch (JAXBException | IOException | XAttrException e) {
        throw new MigrationException("error", e);
      }
    };
  }
  
  private CheckedConsumer<ProjectParams, RollbackException> projectRollback(DistributedFileSystemOps dfso) {
    return params -> {
      try {
        String projectPath = getProjectPath(params.projectName);
        if(!dfso.isDir(projectPath)) {
          LOGGER.warn("project with no directory:{}", projectPath);
          return;
        }
       
        HopsClient.removeXAttr(dfso, projectPath, "provenance.core");
        ElasticClient.deleteProvenanceProjectIndex(httpClient, elastic, params.projectIId, elasticUser, elasticPass);
      } catch (IOException e) {
        throw new RollbackException("error", e);
      }
    };
  }
  
  private CheckedConsumer<DatasetParams, RollbackException> datasetRollback(DistributedFileSystemOps dfso) {
    return params -> {
      String datasetPath = getDatasetPath(params.projectIId, params.projectName, params.datasetPId, params.datasetName);
      try {
        if(!dfso.isDir(datasetPath)) {
          LOGGER.warn("dataset with no directory:{}", datasetPath);
          return;
        }
        if (params.metaStatus == 2 || params.metaStatus == 3) {
          dfso.setMetaStatus(datasetPath, Inode.MetaStatus.META_ENABLED);
        }
        HopsClient.removeXAttr(dfso, datasetPath, "provenance.core");
      } catch ( IOException e) {
        throw new RollbackException("error", e);
      }
    };
  }
  
  private String getProjectPath(String projectName) {
    return "/Projects/" + projectName;
  }
  private String getDatasetPath(long projectIID, String projectName, long datasetPId, String datasetName) {
    if(datasetName.endsWith(".db") && datasetPId != projectIID) {
      return "/apps/hive/warehouse" + "/" + datasetName;
    } else {
      return getProjectPath(projectName) + "/" + datasetName;
    }
  }
  
  private static class ProjectParams {
    long projectIId;
    String projectName;
    byte metaStatus;
    
    public static ProjectParams instance(ResultSet allProjectsResultSet, ResultSet projectInodeResultSet)
      throws SQLException {
      ProjectParams params = new ProjectParams();
      params.projectName = allProjectsResultSet.getString(GET_ALL_PROJECTS_S_INODE_NAME);
      params.projectIId = projectInodeResultSet.getLong(GET_INODE_S_ID);
      params.metaStatus = projectInodeResultSet.getByte(GET_INODE_S_META_ENABLED);
      return params;
    }
  }
  
  private static class DatasetParams {
    long projectIId;
    String projectName;
    String datasetName;
    long datasetPId;
    long datasetPartitionId;
    byte metaStatus;
  
    public static DatasetParams instance(ProjectParams projectParams, ResultSet allProjectDatasetsResultSet,
      ResultSet datasetInodeResultSet) throws SQLException {
      DatasetParams params = new DatasetParams();
      params.projectIId = projectParams.projectIId;
      params.projectName = projectParams.projectName;
      params.datasetPId = allProjectDatasetsResultSet.getLong(GET_PROJECT_DATASETS_S_INODE_PID);
      params.datasetName = allProjectDatasetsResultSet.getString(GET_PROJECT_DATASETS_S_INODE_NAME);
      params.datasetPartitionId = allProjectDatasetsResultSet.getLong(GET_PROJECT_DATASETS_S_PARTITION_ID);
      params.metaStatus = datasetInodeResultSet.getByte(GET_INODE_S_META_ENABLED);
      return params;
    }
  }
  
  private PreparedStatement getProjectInodeStmt(ResultSet allProjectsResultSet) throws SQLException {
    PreparedStatement projectInodeStmt = connection.prepareStatement(GET_INODE);
    projectInodeStmt.setLong(GET_INODE_W_PARTITION_ID,
      allProjectsResultSet.getLong(GET_ALL_PROJECTS_S_PARTITION_ID));
    projectInodeStmt.setLong(GET_INODE_W_PARENT_ID,
      allProjectsResultSet.getLong(GET_ALL_PROJECTS_S_INODE_PID));
    projectInodeStmt.setString(GET_INODE_W_NAME,
      allProjectsResultSet.getString(GET_ALL_PROJECTS_S_INODE_NAME));
    return projectInodeStmt;
  }
  
  private PreparedStatement getDatasetInodeStmt(ResultSet allProjectDatasetsResultSet) throws SQLException {
    PreparedStatement projectInodeStmt = connection.prepareStatement(GET_INODE);
    projectInodeStmt.setLong(GET_INODE_W_PARTITION_ID,
      allProjectDatasetsResultSet.getLong(GET_PROJECT_DATASETS_S_PARTITION_ID));
    projectInodeStmt.setLong(GET_INODE_W_PARENT_ID,
      allProjectDatasetsResultSet.getLong(GET_PROJECT_DATASETS_S_INODE_PID));
    projectInodeStmt.setString(GET_INODE_W_NAME,
      allProjectDatasetsResultSet.getString(GET_PROJECT_DATASETS_S_INODE_NAME));
    return projectInodeStmt;
  }
  
  private PreparedStatement getProjectDatasetsStmt(ResultSet allProjectsResultSet) throws SQLException {
    PreparedStatement allProjectDatasetsStmt = connection.prepareStatement(GET_PROJECT_DATASETS);
    allProjectDatasetsStmt.setInt(GET_PROJECT_DATASETS_W_PROJECT_ID,
      allProjectsResultSet.getInt(GET_ALL_PROJECTS_S_ID));
    return allProjectDatasetsStmt;
  }
  
  private JAXBContext jaxbContext() throws JAXBException {
    Map<String, Object> properties = new HashMap<>();
    properties.put(MarshallerProperties.JSON_INCLUDE_ROOT, false);
    properties.put(MarshallerProperties.MEDIA_TYPE, MediaType.APPLICATION_JSON);
    JAXBContext context = JAXBContextFactory.createContext(
      new Class[] {
        ProvCoreDTO.class,
        ProvTypeDTO.class,
        ProvFeatureDTO.class
      },
      properties);
    return context;
  }

  private String jaxbParser(JAXBContext jaxbContext, ProvCoreDTO provCore) throws JAXBException {
    Marshaller marshaller = jaxbContext.createMarshaller();
    StringWriter sw = new StringWriter();
    marshaller.marshal(provCore, sw);
    return sw.toString();
  }
}

