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

package io.hops.hopsworks.expat.migrations.projects.search.featurestore;

import io.hops.hopsworks.common.hdfs.DistributedFileSystemOps;
import io.hops.hopsworks.common.provenance.core.dto.ProvCoreDTO;
import io.hops.hopsworks.common.provenance.core.dto.ProvTypeDTO;
import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import io.hops.hopsworks.expat.db.DbConnectionFactory;
import io.hops.hopsworks.expat.migrations.MigrateStep;
import io.hops.hopsworks.expat.migrations.MigrationException;
import io.hops.hopsworks.expat.migrations.RollbackException;
import io.hops.hopsworks.expat.migrations.projects.util.HopsClient;
import io.hops.hopsworks.expat.migrations.projects.util.XAttrException;
import io.hops.hopsworks.expat.migrations.projects.util.XAttrHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.persistence.jaxb.JAXBContextFactory;
import org.eclipse.persistence.jaxb.MarshallerProperties;
import org.eclipse.persistence.oxm.MediaType;
import org.opensearch.common.CheckedBiConsumer;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import java.io.StringReader;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class UpdateFeaturegroupsForType implements MigrateStep {

  private final static Logger LOGGER = LogManager.getLogger(UpdateFeaturegroupsForType.class);

  private final static String GET_ALL_FEATURESTORES = "SELECT id, project_id FROM feature_store";
  private final static int GET_ALL_FEATURESTORES_S_ID = 1;
  private final static int GET_ALL_FEATURESTORES_S_PROJECT_ID = 2;
  private final static String GET_ALL_FEATUREGROUPS =
          "SELECT name, version, feature_group_type FROM hopsworks.feature_group WHERE feature_store_id=?";
  private final static int GET_ALL_FEATUREGROUPS_W_FS_ID = 1;
  private final static int GET_ALL_FEATUREGROUPS_S_NAME = 1;
  private final static int GET_ALL_FEATUREGROUPS_S_VERSION = 2;
  private final static int GET_ALL_FEATUREGROUPS_S_TYPE = 3;
  private final static String GET_PROJECT = "SELECT inode_name FROM project WHERE id=?";
  private final static int GET_PROJECT_W_ID = 1;
  private final static int GET_PROJECT_S_NAME = 1;

  protected Connection connection = null;
  DistributedFileSystemOps dfso = null;
  private String hopsUser;
  JAXBContext jaxbContext;
  boolean dryrun = false;

  private void setup() throws ConfigurationException, SQLException, JAXBException {
    jaxbContext = jaxbContext();

    connection = DbConnectionFactory.getConnection();

    Configuration conf = ConfigurationBuilder.getConfiguration();
    hopsUser = conf.getString(ExpatConf.HOPS_CLIENT_USER);
    if (hopsUser == null) {
      throw new ConfigurationException(ExpatConf.HOPS_CLIENT_USER + " cannot be null");
    }
    dfso = HopsClient.getDFSO(hopsUser);
    dryrun = conf.getBoolean(ExpatConf.DRY_RUN);
  }

  private void close() throws SQLException {
    if(connection != null) {
      connection.close();
    }
    if(dfso != null) {
      dfso.close();
    }
  }

  @Override
  public void migrate() throws MigrationException {
    LOGGER.info("featuregroup type migration");
    try {
      setup();
      if(dryrun) {
        traverseElements(dryRunFeaturegroup());
      } else {
        traverseElements(migrateFeaturegroup());
      }
    } catch (Exception e) {
      throw new MigrationException("error", e);
    } finally {
      try {
        close();
      } catch (SQLException e) {
        throw new MigrationException("error", e);
      }
    }
  }

  @Override
  public void rollback() throws RollbackException {
    LOGGER.info("featuregroup type rollback");
    try {
      setup();
      if(dryrun) {
        traverseElements(dryRunFeaturegroup());
      } else {
        traverseElements(revertFeaturegroup());
      }
    } catch (Exception e) {
      throw new RollbackException("error", e);
    } finally {
      try {
        close();
      } catch (SQLException e) {
        throw new RollbackException("error", e);
      }
    }
  }

  private void traverseElements(CheckedBiConsumer<ResultSet, ResultSet, Exception> action) throws Exception {
    PreparedStatement allFeaturestoresStmt = null;
    PreparedStatement allFSFeaturegroupsStmt = null;

    try {
      connection.setAutoCommit(false);
      allFeaturestoresStmt = connection.prepareStatement(GET_ALL_FEATURESTORES);
      ResultSet allFeaturestoresResultSet = allFeaturestoresStmt.executeQuery();

      while (allFeaturestoresResultSet.next()) {
        allFSFeaturegroupsStmt = getFSFeaturegroupsStmt(allFeaturestoresResultSet);
        ResultSet allFSFeaturegroupsResultSet = allFSFeaturegroupsStmt.executeQuery();
        while(allFSFeaturegroupsResultSet.next()) {
          action.accept(allFeaturestoresResultSet, allFSFeaturegroupsResultSet);
        }
        allFSFeaturegroupsStmt.close();
      }
      allFeaturestoresStmt.close();
      connection.commit();
      connection.setAutoCommit(true);
    } finally {
      if(allFSFeaturegroupsStmt != null) {
        allFSFeaturegroupsStmt.close();
      }
      if(allFeaturestoresStmt != null) {
        allFeaturestoresStmt.close();
      }
      close();
    }
  }

  private CheckedBiConsumer<ResultSet, ResultSet, Exception> dryRunFeaturegroup() {
    return (ResultSet allFeaturestoresResultSet, ResultSet allFSFeaturegroupsResultSet) -> {
      String projectName = getProjectName(allFeaturestoresResultSet);
      String featuregroupName = allFSFeaturegroupsResultSet.getString(GET_ALL_FEATUREGROUPS_S_NAME);
      int featuregroupVersion = allFSFeaturegroupsResultSet.getInt(GET_ALL_FEATUREGROUPS_S_VERSION);
      int featuregroupType = allFSFeaturegroupsResultSet.getInt(GET_ALL_FEATUREGROUPS_S_TYPE);
      String featuregroupPath = getFeaturegroupPath(projectName, featuregroupName, featuregroupVersion);
      LOGGER.info("featuregroup:{}", featuregroupPath);

      byte[] existingVal = dfso.getXAttr(new Path(featuregroupPath), "provenance.featurestore");
      FeaturegroupXAttr.FullDTO existingXattr = jaxbUnmarshal(jaxbContext, existingVal);

      if(existingXattr == null) {
        LOGGER.info("featuregroup:{} rollbacked (no xattr fg value)", featuregroupPath);
      } else if(existingXattr.getFgType() == null) {
        LOGGER.info("featuregroup:{} rollbacked (no xattr fg type value)", featuregroupPath);
      } else {
        FeaturegroupXAttr.FGType fgType = null;
        switch(featuregroupType) {
          case 0: // Cache
            fgType = FeaturegroupXAttr.FGType.CACHED;
            break;
          case 1: // On-demand
            fgType = FeaturegroupXAttr.FGType.ON_DEMAND;
            break;
        }
        if(existingXattr.getFgType().equals(fgType)) {
          LOGGER.info("featuregroup:{} migrated (correct fg type value)", featuregroupPath);
        } else {
          LOGGER.info("featuregroup:{} bad fg type value", featuregroupPath);
        }
      }
    };
  }

  private CheckedBiConsumer<ResultSet, ResultSet, Exception> migrateFeaturegroup() {
    return (ResultSet allFeaturestoresResultSet, ResultSet allFSFeaturegroupsResultSet) -> {
      String projectName = getProjectName(allFeaturestoresResultSet);
      String featuregroupName = allFSFeaturegroupsResultSet.getString(GET_ALL_FEATUREGROUPS_S_NAME);
      int featuregroupVersion = allFSFeaturegroupsResultSet.getInt(GET_ALL_FEATUREGROUPS_S_VERSION);
      int featuregroupType = allFSFeaturegroupsResultSet.getInt(GET_ALL_FEATUREGROUPS_S_TYPE);
      String featuregroupPath = getFeaturegroupPath(projectName, featuregroupName, featuregroupVersion);
      LOGGER.info("featuregroup:{}", featuregroupPath);

      byte[] existingVal = dfso.getXAttr(new Path(featuregroupPath), "provenance.featurestore");
      FeaturegroupXAttr.FullDTO xattr = jaxbUnmarshal(jaxbContext, existingVal);
      if(xattr == null) {
        LOGGER.info("featuregroup:{} no xattr fg value", featuregroupPath);
        return;
      }
      switch(featuregroupType) {
        case 0: // Cache
          xattr.setFgType(FeaturegroupXAttr.FGType.CACHED);
          break;
        case 1: // On-demand
          xattr.setFgType(FeaturegroupXAttr.FGType.ON_DEMAND);
          break;
      }
      byte[] val = jaxbMarshal(jaxbContext, xattr).getBytes();
      if (val.length > 13500) {
        LOGGER.warn("xattr too large - skipping attaching features to featuregroup:{}", featuregroupPath);
        xattr = new FeaturegroupXAttr.FullDTO(
                xattr.getFeaturestoreId(), xattr.getDescription(), xattr.getCreateDate(), xattr.getCreator());
        val = jaxbMarshal(jaxbContext, xattr).getBytes();
      }
      try{
        XAttrHelper.upsertProvXAttr(dfso, featuregroupPath, "featurestore", val);
        LOGGER.info("featuregroup:{} successfully migrated to having fg type", featuregroupPath);
      } catch (XAttrException e) {
        throw e;
      }
    };
  }

  private CheckedBiConsumer<ResultSet, ResultSet, Exception> revertFeaturegroup() {
    return (ResultSet allFeaturestoresResultSet, ResultSet allFSFeaturegroupsResultSet) -> {
      String projectName = getProjectName(allFeaturestoresResultSet);
      String featuregroupName = allFSFeaturegroupsResultSet.getString(GET_ALL_FEATUREGROUPS_S_NAME);
      int featuregroupVersion = allFSFeaturegroupsResultSet.getInt(GET_ALL_FEATUREGROUPS_S_VERSION);
      String featuregroupPath = getFeaturegroupPath(projectName, featuregroupName, featuregroupVersion);
      LOGGER.info("featuregroup:{}", featuregroupPath);

      byte[] existingVal = dfso.getXAttr(new Path(featuregroupPath), "provenance.featurestore");
      FeaturegroupXAttr.FullDTO xattr = jaxbUnmarshal(jaxbContext, existingVal);
      if(xattr == null) {
        LOGGER.info("featuregroup:{} no xattr fg value", featuregroupPath);
        return;
      }
      xattr.setFgType(null);
      byte[] val = jaxbMarshal(jaxbContext, xattr).getBytes();
      try{
        XAttrHelper.upsertProvXAttr(dfso, featuregroupPath, "featurestore", val);
        LOGGER.info("featuregroup:{} successfully rolled back from having fg type", featuregroupPath);
      } catch (XAttrException e) {
        throw e;
      }
    };
  }

  private PreparedStatement getFSFeaturegroupsStmt(ResultSet allFeaturestoresResultSet) throws SQLException {
    PreparedStatement stmt = connection.prepareStatement(GET_ALL_FEATUREGROUPS);
    stmt.setInt(GET_ALL_FEATUREGROUPS_W_FS_ID, allFeaturestoresResultSet.getInt(GET_ALL_FEATURESTORES_S_ID));
    return stmt;
  }

  private PreparedStatement getProjectStmt(ResultSet allFeaturestoresResultSet) throws SQLException {
    PreparedStatement stmt = connection.prepareStatement(GET_PROJECT);
    stmt.setInt(GET_PROJECT_W_ID, allFeaturestoresResultSet.getInt(GET_ALL_FEATURESTORES_S_PROJECT_ID));
    return stmt;
  }

  private String getProjectName(ResultSet allFeaturestoreResultSet) throws SQLException {
    PreparedStatement projectStmt = null;
    try {
      projectStmt = getProjectStmt(allFeaturestoreResultSet);
      ResultSet projectsResultSet = projectStmt.executeQuery();
      if (projectsResultSet.next()) {
        return projectsResultSet.getString(GET_PROJECT_S_NAME);
      } else {
        throw new IllegalStateException("project parent not found");
      }
    } finally {
      if(projectStmt != null) {
        projectStmt.close();
      }
    }
  }

  private JAXBContext jaxbContext() throws JAXBException {
    Map<String, Object> properties = new HashMap<>();
    properties.put(MarshallerProperties.JSON_INCLUDE_ROOT, false);
    properties.put(MarshallerProperties.MEDIA_TYPE, MediaType.APPLICATION_JSON);
    JAXBContext context = JAXBContextFactory.createContext(
            new Class[] {
                ProvCoreDTO.class,
                ProvTypeDTO.class,
                FeaturegroupXAttr.FullDTO.class
            },
            properties);
    return context;
  }

  private FeaturegroupXAttr.FullDTO jaxbUnmarshal(JAXBContext jaxbContext, byte[] val) throws JAXBException {
    Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
    StreamSource ss = new StreamSource(new StringReader(new String(val)));
    return unmarshaller.unmarshal(ss, FeaturegroupXAttr.FullDTO.class).getValue();
  }

  private String jaxbMarshal(JAXBContext jaxbContext, FeaturegroupXAttr.FullDTO xattr) throws JAXBException {
    Marshaller marshaller = jaxbContext.createMarshaller();
    StringWriter sw = new StringWriter();
    marshaller.marshal(xattr, sw);
    return sw.toString();
  }

  private String getFeaturegroupPath(String project, String featuregroup, int version) {
    return "/apps/hive/warehouse/" + project.toLowerCase() + "_featurestore.db/" + featuregroup + "_" + version;
  }
}
