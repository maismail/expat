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

package io.hops.hopsworks.expat.migrations.featurestore.featuregroup;

import io.hops.hopsworks.common.hdfs.DistributedFileSystemOps;
import io.hops.hopsworks.common.provenance.core.dto.ProvCoreDTO;
import io.hops.hopsworks.common.provenance.core.dto.ProvTypeDTO;
import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import io.hops.hopsworks.expat.db.DbConnectionFactory;
import io.hops.hopsworks.expat.epipe.EpipeRunner;
import io.hops.hopsworks.expat.migrations.MigrateStep;
import io.hops.hopsworks.expat.migrations.MigrationException;
import io.hops.hopsworks.expat.migrations.RollbackException;
import io.hops.hopsworks.expat.migrations.projects.search.featurestore.FeaturegroupXAttr;
import io.hops.hopsworks.expat.migrations.projects.util.HopsClient;
import io.hops.hopsworks.expat.migrations.projects.util.XAttrException;
import io.hops.hopsworks.expat.migrations.projects.util.XAttrHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.hadoop.fs.Path;
import org.apache.http.HttpHost;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.persistence.jaxb.JAXBContextFactory;
import org.eclipse.persistence.jaxb.MarshallerProperties;
import org.eclipse.persistence.oxm.MediaType;
import org.opensearch.common.CheckedBiConsumer;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

public class UpdateFeatureGroupFeatureDescription implements MigrateStep {
  private final static Logger LOGGER = LogManager.getLogger(UpdateFeatureGroupFeatureDescription.class);
  
  private final static String GET_ALL_FEATURESTORES = "SELECT id, project_id FROM feature_store";
  private final static int GET_ALL_FEATURESTORES_S_ID = 1;
  private final static int GET_ALL_FEATURESTORES_S_PROJECT_ID = 2;
  private final static String GET_HIVE_MANAGED_FEATUREGROUPS =
    "SELECT f.name, f.version, f.created, f.creator, t.TBL_ID, s.LOCATION, c.id " +
    "FROM hopsworks.feature_group f JOIN hopsworks.cached_feature_group c JOIN metastore.TBLS t JOIN metastore.SDS s " +
      "ON f.cached_feature_group_id=c.id AND c.offline_feature_group = t.TBL_ID AND t.SD_ID=s.SD_ID " +
      "WHERE t.TBL_TYPE = \"MANAGED_TABLE\" AND feature_store_id=?";
  private final static int GET_HIVE_MANAGED_FEATUREGROUPS_W_FS_ID = 1;
  private final static int GET_HIVE_MANAGED_FEATUREGROUPS_S_NAME = 1;
  private final static int GET_HIVE_MANAGED_FEATUREGROUPS_S_VERSION = 2;
  private final static int GET_HIVE_MANAGED_FEATUREGROUPS_S_CREATED = 3;
  private final static int GET_HIVE_MANAGED_FEATUREGROUPS_S_CREATOR = 4;
  private final static int GET_HIVE_MANAGED_FEATUREGROUPS_S_TBL_ID = 5;
  private final static int GET_HIVE_MANAGED_FEATUREGROUPS_S_LOCATION = 6;
  private final static int GET_HIVE_MANAGED_FEATUREGROUPS_S_ID = 7;
  
  private final static String GET_USER = "SELECT email FROM users WHERE uid=?";
  private final static int GET_USER_W_ID = 1;
  private final static int GET_USER_S_EMAIL = 1;
  private final static String GET_FG_DESCRIPTION =
    "SELECT PARAM_VALUE FROM metastore.TABLE_PARAMS " +
      "WHERE TBL_ID=? AND PARAM_KEY=?";
  private final static int GET_FG_DESCRIPTION_W_TBL_ID = 1;
  private final static int GET_FG_DESCRIPTION_W_PARAM = 2;
  private final static int GET_FG_DESCRIPTION_S_DESCRIPTION = 1;
  private final static String GET_FG_FEATURES =
    "SELECT c.COLUMN_NAME, c.COMMENT FROM metastore.TBLS t " +
      "JOIN metastore.SDS s JOIN metastore.COLUMNS_V2 c " +
      "ON t.SD_ID=s.SD_ID AND s.CD_ID=c.CD_ID WHERE t.TBL_ID = ?";
  private final static String GET_FG_PARTITION_FEATURES =
    "SELECT t.PKEY_NAME, t.PKEY_COMMENT from metastore.PARTITION_KEYS t " +
      "WHERE t.TBL_ID = ?";
  private final static int GET_FG_FEATURES_W_TBL_ID = 1;
  private final static int GET_FG_FEATURES_S_NAME = 1;
  private final static int GET_FG_FEATURES_S_COMMENT = 2;
  private final static String GET_PROJECT = "SELECT inode_name FROM project WHERE id=?";
  private final static int GET_PROJECT_W_ID = 1;
  private final static int GET_PROJECT_S_NAME = 1;
  private final static String INSERT_CACHED_FG_DESC = "INSERT INTO cached_feature " +
    "(cached_feature_group_id, name, description) VALUES (?,?,?)";
  private final static int INSERT_CACHED_FG_DESC_W_FG_ID = 1;
  private final static int INSERT_CACHED_FG_DESC_W_NAME = 2;
  private final static int INSERT_CACHED_FG_DESC_W_DESC = 3;
  private final static String GET_ALL_ON_DEMAND_FGS = "SELECT f.name, f.version, f.created, f.creator, " +
    "o.id, o.description " +
    "FROM feature_group f JOIN on_demand_feature_group o ON f.on_demand_feature_group_id=o.id " +
    "WHERE f.feature_group_type=1 AND f.feature_store_id=?";
  private final static int GET_ALL_ON_DEMAND_FGS_S_NAME = 1;
  private final static int GET_ALL_ON_DEMAND_FGS_S_VERSION = 2;
  private final static int GET_ALL_ON_DEMAND_FGS_S_CREATED = 3;
  private final static int GET_ALL_ON_DEMAND_FGS_S_CREATOR = 4;
  private final static int GET_ALL_ON_DEMAND_FGS_S_ID = 5;
  private final static int GET_ALL_ON_DEMAND_FGS_S_DESCRIPTION = 6;
  private final static int GET_ALL_ON_DEMAND_FGS_W_FS_ID = 1;
  private final static String GET_ON_DEMAND_FG_FEATURES = "SELECT name, description FROM on_demand_feature WHERE " +
    "on_demand_feature_group_id=?";
  private final static int GET_ON_DEMAND_FG_FEATURES_S_NAME = 1;
  private final static int GET_ON_DEMAND_FG_FEATURES_S_DESCRIPTION = 1;
  private final static int GET_ON_DEMAND_FG_FEATURES_W_ID = 1;
  private final static String DELETE_CACHED_FEATURES = "DELETE FROM cached_features";
  
  private static final List<String> HUDI_SPEC_FEATURE_NAMES = Arrays.asList("_hoodie_record_key",
    "_hoodie_partition_path", "_hoodie_commit_time", "_hoodie_file_name", "_hoodie_commit_seqno");
  
  protected Connection connection = null;
  DistributedFileSystemOps dfso = null;
  private String hopsUser;
  SimpleDateFormat formatter;
  JAXBContext jaxbContextMigrate;
  JAXBContext jaxbContextRollback;
  boolean dryrun = false;
  
  private HttpHost elastic;
  private String elasticUser;
  private String elasticPass;
  private CloseableHttpClient httpClient;
  private String epipeLocation;
  
  private void setup()
    throws ConfigurationException, SQLException, JAXBException, KeyStoreException, NoSuchAlgorithmException,
           KeyManagementException {
    formatter = new SimpleDateFormat("yyyy-M-dd hh:mm:ss", Locale.ENGLISH);
    jaxbContextMigrate = jaxbContextMigrate();
    jaxbContextRollback = jaxbContextRollback();
    
    connection = DbConnectionFactory.getConnection();
    
    Configuration conf = ConfigurationBuilder.getConfiguration();
    hopsUser = conf.getString(ExpatConf.HOPS_CLIENT_USER);
    if (hopsUser == null) {
      throw new ConfigurationException(ExpatConf.HOPS_CLIENT_USER + " cannot be null");
    }
    dfso = HopsClient.getDFSO(hopsUser);
    dryrun = conf.getBoolean(ExpatConf.DRY_RUN);
  
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
  
    epipeLocation = conf.getString(ExpatConf.EPIPE_PATH);
    if (epipeLocation == null) {
      throw new ConfigurationException(ExpatConf.EPIPE_PATH + " cannot be null");
    }

    httpClient = HttpClients
      .custom()
      .setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(
        CookieSpecs.IGNORE_COOKIES).build())
      .setSSLContext(new SSLContextBuilder().loadTrustMaterial((x509Certificates, s) -> true).build())
      .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
      .build();
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
    LOGGER.info("featuregroup feature description migration");
    try {
      setup();
      connection.setAutoCommit(false);
      if(dryrun) {
        traverseElements(dryRunCachedFG(), dryRunOnDemandFG());
      } else {
        EpipeRunner.stopEpipe();
        traverseElements(migrateCachedFG(), migrateOnDemandFG());
        EpipeRunner.reindex(httpClient, elastic, elasticUser, elasticPass, epipeLocation, true, true);
        EpipeRunner.restartEpipe();
      }
      connection.setAutoCommit(true);
    } catch (Throwable e) {
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
    LOGGER.info("featuregroup feature description rollback");
    try {
      setup();
      connection.setAutoCommit(false);
      if(dryrun) {
        traverseElements(dryRunCachedFG(), dryRunOnDemandFG());
      } else {
        EpipeRunner.stopEpipe();
        traverseElements(rollbackCachedFG(), rollbackOnDemandFG());
        deleteCachedFeatures();
        EpipeRunner.reindex(httpClient, elastic, elasticUser, elasticPass, epipeLocation, true, true);
        EpipeRunner.restartEpipe();
      }
      connection.setAutoCommit(true);
    } catch (Throwable e) {
      throw new RollbackException("error", e);
    } finally {
      try {
        close();
      } catch (SQLException e) {
        throw new RollbackException("error", e);
      }
    }
  }
  
  private void deleteCachedFeatures() throws SQLException {
    PreparedStatement deleteCachedFeatures = null;
    try {
      deleteCachedFeatures = connection.prepareStatement(DELETE_CACHED_FEATURES);
      deleteCachedFeatures.executeUpdate();
    } finally {
      if(deleteCachedFeatures != null) {
        deleteCachedFeatures.close();
      }
    }
  }
  
  private void traverseElements(CheckedBiConsumer<ResultSet, ResultSet, Exception> cachedFGAction,
                                CheckedBiConsumer<ResultSet, ResultSet, Exception> onDemandFGAction)
    throws Exception {
    PreparedStatement allFSStmt = null;
    PreparedStatement allFSCachedFGStmt = null;
    PreparedStatement allFSOnDemandFGStmt = null;
    
    try {
      allFSStmt = connection.prepareStatement(GET_ALL_FEATURESTORES);
      ResultSet allFSResultSet = allFSStmt.executeQuery();
      
      while (allFSResultSet.next()) {
        allFSCachedFGStmt = getFSCachedFGStmt(allFSResultSet);
        ResultSet allFSCachedFGResultSet = allFSCachedFGStmt.executeQuery();
        while(allFSCachedFGResultSet.next()) {
          cachedFGAction.accept(allFSResultSet, allFSCachedFGResultSet);
        }
        allFSCachedFGStmt.close();

        allFSOnDemandFGStmt = getFSOnDemandFGStmt(allFSResultSet);
        ResultSet allFSOnDemandFGResultSet = allFSOnDemandFGStmt.executeQuery();
        while(allFSOnDemandFGResultSet.next()) {
          onDemandFGAction.accept(allFSResultSet, allFSOnDemandFGResultSet);
        }
        allFSOnDemandFGStmt.close();
      }
      connection.commit();
    } finally {
      if(allFSCachedFGStmt != null) {
        allFSCachedFGStmt.close();
      }
      if(allFSStmt != null) {
        allFSStmt.close();
      }
      if(allFSOnDemandFGStmt != null) {
        allFSOnDemandFGStmt.close();
      }
    }
  }
  
  private CheckedBiConsumer<ResultSet, ResultSet, Exception> dryRunCachedFG() {
    return (ResultSet allFSResultSet, ResultSet allFSCachedFGResultSet) -> {
      String fgPath = getCachedFGPath(allFSResultSet, allFSCachedFGResultSet);
      if (fgPath == null) {
        LOGGER.error("faulty cached featuregroup:{}", fgPath);
      } else {
        LOGGER.info("cached featuregroup:{}", fgPath);
        PreparedStatement insertCachedFeaturesStmt = connection.prepareStatement(INSERT_CACHED_FG_DESC);
        FeaturegroupXAttrV2.FullDTO xattr = baseCachedFGXAttr(allFSResultSet, allFSCachedFGResultSet);
        List<FeaturegroupXAttrV2.SimpleFeatureDTO> features =
          getCachedFeaturesMigrate(allFSCachedFGResultSet, insertCachedFeaturesStmt);
  
        dryRunMigrateXAttr(fgPath, xattr, features);
        try {
          LOGGER.info("cached feature desc:{}", insertCachedFeaturesStmt.toString());
        } finally {
          insertCachedFeaturesStmt.close();
        }
      }
    };
  }
  
  private CheckedBiConsumer<ResultSet, ResultSet, Exception> dryRunOnDemandFG() {
    return (ResultSet allFSResultSet, ResultSet allFSOnDemandFGResultSet) -> {
      String fgPath = getOnDemandFGPath(allFSResultSet, allFSOnDemandFGResultSet);
      LOGGER.info("on demand featuregroup:{}", fgPath);
      FeaturegroupXAttrV2.FullDTO xattr = baseOnDemandFGXAttr(allFSResultSet, allFSOnDemandFGResultSet);
      List<FeaturegroupXAttrV2.SimpleFeatureDTO> features = getOnDemandFeaturesMigrate(allFSOnDemandFGResultSet);
      dryRunMigrateXAttr(fgPath, xattr, features);
    };
  }
  
  private CheckedBiConsumer<ResultSet, ResultSet, Exception> migrateCachedFG() {
    return (ResultSet allFSResultSet, ResultSet allFSCachedFGResultSet) -> {
      String fgPath = getCachedFGPath(allFSResultSet, allFSCachedFGResultSet);
      if(fgPath == null) {
        LOGGER.error("faulty cached featuregroup:{}", fgPath);
      } else {
        PreparedStatement insertCachedFeaturesStmt = connection.prepareStatement(INSERT_CACHED_FG_DESC);
        LOGGER.info("cached featuregroup:{}", fgPath);
        FeaturegroupXAttrV2.FullDTO xattr = baseCachedFGXAttr(allFSResultSet, allFSCachedFGResultSet);
        List<FeaturegroupXAttrV2.SimpleFeatureDTO> features =
          getCachedFeaturesMigrate(allFSCachedFGResultSet, insertCachedFeaturesStmt);
        xattr.setFeatures(features);
        migrateXAttr(fgPath, xattr);
        try {
          insertCachedFeaturesStmt.executeBatch();
        } finally {
          insertCachedFeaturesStmt.close();
        }
      }
    };
  }
  
  private CheckedBiConsumer<ResultSet, ResultSet, Exception> migrateOnDemandFG() {
    return (ResultSet allFSResultSet, ResultSet allFSOnDemandFGResultSet) -> {
      String fgPath = getOnDemandFGPath(allFSResultSet, allFSOnDemandFGResultSet);
      LOGGER.info("on demand featuregroup:{}", fgPath);
      FeaturegroupXAttrV2.FullDTO xattr = baseOnDemandFGXAttr(allFSResultSet, allFSOnDemandFGResultSet);
      List<FeaturegroupXAttrV2.SimpleFeatureDTO> features = getOnDemandFeaturesMigrate(allFSOnDemandFGResultSet);
      xattr.setFeatures(features);
      migrateXAttr(fgPath, xattr);
    };
  }
  
  public CheckedBiConsumer<ResultSet, ResultSet, Exception> rollbackCachedFG() {
    return (ResultSet allFSResultSet, ResultSet allFSCachedFGResultSet) -> {
      String fgPath = getCachedFGPath(allFSResultSet, allFSCachedFGResultSet);
      if(fgPath != null) {
        LOGGER.info("cached featuregroup:{}", fgPath);
        FeaturegroupXAttr.FullDTO xattr = readRollbackXAttr(fgPath);
        rollbackXAttr(fgPath, xattr);
      }
    };
  }
  
  private CheckedBiConsumer<ResultSet, ResultSet, Exception> rollbackOnDemandFG() {
    return (ResultSet allFSResultSet, ResultSet allFSOnDemandFGResultSet) -> {
      String fgPath = getOnDemandFGPath(allFSResultSet, allFSOnDemandFGResultSet);
      LOGGER.info("on demand featuregroup:{}", fgPath);
      FeaturegroupXAttr.FullDTO xattr = readRollbackXAttr(fgPath);
      rollbackXAttr(fgPath, xattr);
    };
  }
  
  private FeaturegroupXAttr.FullDTO readRollbackXAttr(String fgPath) throws JAXBException, IOException {
    byte[] val = dfso.getXAttr(new Path(fgPath), "provenance.featurestore");
    if(val == null) {
      LOGGER.warn("featuregroup:{} had no value", fgPath);
      return null;
    } else {
      FeaturegroupXAttrV2.FullDTO mVal = FeaturegroupXAttrV2.jaxbUnmarshal(jaxbContextMigrate, val);
      FeaturegroupXAttr.FullDTO rVal = new FeaturegroupXAttr.FullDTO(mVal.getFeaturestoreId(), mVal.getDescription(),
        mVal.getCreateDate(), mVal.getCreator());
      List<String> features = new LinkedList<>();
      rVal.setFeatures(features);
      for(FeaturegroupXAttrV2.SimpleFeatureDTO feature : mVal.getFeatures()) {
        features.add(feature.getName());
      }
      return rVal;
    }
  }
  
  private String getCachedFGPath(ResultSet allFSResultSet, ResultSet allFSCachedFGResultSet) throws SQLException {
    String projectName = getProjectName(allFSResultSet);
    String featuregroupName = allFSCachedFGResultSet.getString(GET_HIVE_MANAGED_FEATUREGROUPS_S_NAME);
    int featuregroupVersion = allFSCachedFGResultSet.getInt(GET_HIVE_MANAGED_FEATUREGROUPS_S_VERSION);
    String featuregroupLocation = allFSCachedFGResultSet.getString(GET_HIVE_MANAGED_FEATUREGROUPS_S_LOCATION);
    String featuregroupPath = getFeaturegroupPath(projectName, featuregroupName, featuregroupVersion);
    if (!featuregroupLocation.endsWith(featuregroupPath)) {
      LOGGER.warn("location mismatch - table:{} computed:{}", featuregroupLocation, featuregroupPath);
      return null;
    }
    return featuregroupPath;
  }
  
  private String getOnDemandFGPath(ResultSet allFSResultSet, ResultSet allFSOnDemandFGResultSet)
    throws SQLException {
    String projectName = getProjectName(allFSResultSet);
    String featuregroupName = allFSOnDemandFGResultSet.getString(GET_ALL_ON_DEMAND_FGS_S_NAME);
    int featuregroupVersion = allFSOnDemandFGResultSet.getInt(GET_ALL_ON_DEMAND_FGS_S_VERSION);
    String featuregroupPath = getFeaturegroupPath(projectName, featuregroupName, featuregroupVersion);
    
    return featuregroupPath;
  }
  
  private FeaturegroupXAttrV2.FullDTO baseCachedFGXAttr(ResultSet allFSResultSet, ResultSet allFSCachedFGResultSet)
    throws SQLException, ParseException {
    
    int featurestoreId = allFSResultSet.getInt(GET_ALL_FEATURESTORES_S_ID);
    String description = getDescription(allFSCachedFGResultSet);
    Date createDate = formatter.parse(allFSCachedFGResultSet.getString(GET_HIVE_MANAGED_FEATUREGROUPS_S_CREATED));
    int userId = allFSCachedFGResultSet.getInt(GET_HIVE_MANAGED_FEATUREGROUPS_S_CREATOR);
    String creator = getCreator(userId);
    
    return new FeaturegroupXAttrV2.FullDTO(featurestoreId, description, createDate.getTime(), creator);
  }
  
  private FeaturegroupXAttrV2.FullDTO baseOnDemandFGXAttr(ResultSet allFSResultSet, ResultSet allFSOnDemandFGResultSet)
    throws SQLException, ParseException {
    int featurestoreId = allFSResultSet.getInt(GET_ALL_FEATURESTORES_S_ID);
    String description = allFSOnDemandFGResultSet.getString(GET_ALL_ON_DEMAND_FGS_S_DESCRIPTION);
    Date createDate = formatter.parse(allFSOnDemandFGResultSet.getString(GET_ALL_ON_DEMAND_FGS_S_CREATED));
    int userId = allFSOnDemandFGResultSet.getInt(GET_ALL_ON_DEMAND_FGS_S_CREATOR);
    String creator = getCreator(userId);
    
    return new FeaturegroupXAttrV2.FullDTO(featurestoreId, description, createDate.getTime(), creator);
  }
  
  private void dryRunMigrateXAttr(String fgPath,
                                  FeaturegroupXAttrV2.FullDTO xattr,
                                  List<FeaturegroupXAttrV2.SimpleFeatureDTO> features)
    throws JAXBException, IOException {
    
    xattr.setFeatures(features);
    byte[] val = FeaturegroupXAttrV2.jaxbMarshal(jaxbContextMigrate, xattr).getBytes();
    if (val.length > 13500) {
      LOGGER.warn("xattr too large - skipping attaching features to featuregroup:{}", fgPath);
      xattr.setFeatures(new LinkedList<>());
    }
  
    byte[] existingVal = dfso.getXAttr(new Path(fgPath), "provenance.featurestore");
    if(existingVal == null) {
      LOGGER.warn("no value:{}", fgPath);
    } else {
      LOGGER.debug("old:{}", new String(existingVal));
      LOGGER.debug("new:{}", FeaturegroupXAttrV2.jaxbMarshal(jaxbContextMigrate, xattr));
    }
  }
  
  private void migrateXAttr(String fgPath, FeaturegroupXAttrV2.FullDTO xattr)
    throws JAXBException, XAttrException {
    byte[] val = FeaturegroupXAttrV2.jaxbMarshal(jaxbContextMigrate, xattr).getBytes();
    if (val.length > 13500) {
      LOGGER.warn("xattr too large - skipping attaching features to featuregroup:{}", fgPath);
      xattr.setFeatures(new LinkedList<>());
      val = FeaturegroupXAttrV2.jaxbMarshal(jaxbContextMigrate, xattr).getBytes();
    }
    XAttrHelper.upsertProvXAttr(dfso, fgPath, "featurestore", val);
  }
  
  
  
  private void rollbackXAttr(String fgPath, FeaturegroupXAttr.FullDTO xattr) throws JAXBException, XAttrException {
    byte[] val = FeaturegroupXAttr.jaxbMarshal(jaxbContextRollback, xattr).getBytes();
    if (val.length > 13500) {
      LOGGER.warn("xattr too large - skipping attaching features to featuregroup:{}", fgPath);
      xattr.setFeatures(new LinkedList<>());
      val = FeaturegroupXAttr.jaxbMarshal(jaxbContextRollback, xattr).getBytes();
    }
    XAttrHelper.upsertProvXAttr(dfso, fgPath, "featurestore", val);
  }
  
  private PreparedStatement getFSCachedFGStmt(ResultSet allFeaturestoresResultSet) throws SQLException {
    PreparedStatement stmt = connection.prepareStatement(GET_HIVE_MANAGED_FEATUREGROUPS);
    stmt.setInt(GET_HIVE_MANAGED_FEATUREGROUPS_W_FS_ID, allFeaturestoresResultSet.getInt(GET_ALL_FEATURESTORES_S_ID));
    return stmt;
  }
  
  private PreparedStatement getFSOnDemandFGStmt(ResultSet allFeaturestoresResultSet) throws SQLException {
    PreparedStatement stmt = connection.prepareStatement(GET_ALL_ON_DEMAND_FGS);
    stmt.setInt(GET_ALL_ON_DEMAND_FGS_W_FS_ID, allFeaturestoresResultSet.getInt(GET_ALL_FEATURESTORES_S_ID));
    return stmt;
  }
  
  private PreparedStatement getFGUserStmt(int userId) throws SQLException {
    PreparedStatement stmt = connection.prepareStatement(GET_USER);
    stmt.setInt(GET_USER_W_ID, userId);
    return stmt;
  }
  private PreparedStatement getFGDescriptionStmt(ResultSet allFSFeaturegroupsResultSet) throws SQLException {
    PreparedStatement stmt = connection.prepareStatement(GET_FG_DESCRIPTION);
    stmt.setInt(GET_FG_DESCRIPTION_W_TBL_ID,
      allFSFeaturegroupsResultSet.getInt(GET_HIVE_MANAGED_FEATUREGROUPS_S_TBL_ID));
    stmt.setString(GET_FG_DESCRIPTION_W_PARAM, "comment");
    return stmt;
  }
  private PreparedStatement getCachedFGFeaturesStmt(String sql, ResultSet allFSFeaturegroupsResultSet)
    throws SQLException {
    PreparedStatement stmt = connection.prepareStatement(sql);
    stmt.setInt(GET_FG_FEATURES_W_TBL_ID, allFSFeaturegroupsResultSet.getInt(GET_HIVE_MANAGED_FEATUREGROUPS_S_TBL_ID));
    return stmt;
  }
  private PreparedStatement getOnDemandFGFeaturesStmt(int fgId) throws SQLException {
    PreparedStatement stmt = connection.prepareStatement(GET_ON_DEMAND_FG_FEATURES);
    stmt.setInt(GET_ON_DEMAND_FG_FEATURES_W_ID, fgId);
    return stmt;
  }
  private PreparedStatement getProjectStmt(ResultSet allFeaturestoresResultSet) throws SQLException {
    PreparedStatement stmt = connection.prepareStatement(GET_PROJECT);
    stmt.setInt(GET_PROJECT_W_ID, allFeaturestoresResultSet.getInt(GET_ALL_FEATURESTORES_S_PROJECT_ID));
    return stmt;
  }
  private String getCreator(int userId) throws SQLException {
    PreparedStatement fgUserStmt = null;
    try {
      fgUserStmt = getFGUserStmt(userId);
      ResultSet fgUserResultSet = fgUserStmt.executeQuery();
      if (fgUserResultSet.next()) {
        return fgUserResultSet.getString(GET_USER_S_EMAIL);
      } else {
        throw new IllegalStateException("featuregroup creator not found");
      }
    } finally {
      if(fgUserStmt != null) {
        fgUserStmt.close();
      }
    }
  }
  
  private String getDescription(ResultSet allFSFeaturegroupsResultSet) throws SQLException {
    PreparedStatement fgDescriptionStmt = null;
    try {
      fgDescriptionStmt = getFGDescriptionStmt(allFSFeaturegroupsResultSet);
      ResultSet fgDescriptionResultSet = fgDescriptionStmt.executeQuery();
      if (fgDescriptionResultSet.next()) {
        return fgDescriptionResultSet.getString(GET_FG_DESCRIPTION_S_DESCRIPTION);
      } else {
        throw new IllegalStateException("featuregroup description not found");
      }
    } finally {
      if(fgDescriptionStmt != null) {
        fgDescriptionStmt.close();
      }
    }
  }
  
  private List<FeaturegroupXAttrV2.SimpleFeatureDTO> getCachedFeaturesMigrate(ResultSet allFSCachedFGResultSet,
                                                                              PreparedStatement insertCachedFStmt)
      throws SQLException {
    List<FeaturegroupXAttrV2.SimpleFeatureDTO> features = new LinkedList<>();
    // regular features
    features.addAll(getCachedFeaturesMigrate(GET_FG_FEATURES, allFSCachedFGResultSet, insertCachedFStmt));
    // features used as partition keys
    features.addAll(getCachedFeaturesMigrate(GET_FG_PARTITION_FEATURES, allFSCachedFGResultSet, insertCachedFStmt));
    // filter hudi columns
    return features.stream()
      .filter(feature -> !HUDI_SPEC_FEATURE_NAMES.contains(feature.getName())).collect(Collectors.toList());
  }

  private List<FeaturegroupXAttrV2.SimpleFeatureDTO> getCachedFeaturesMigrate(String sql,
                                                                              ResultSet allFSCachedFResultSet,
                                                                              PreparedStatement insertCachedFStmt)
      throws SQLException {
    PreparedStatement fgFeaturesStmt = null;
    try {
      fgFeaturesStmt = getCachedFGFeaturesStmt(sql, allFSCachedFResultSet);
      ResultSet fgFeaturesResultSet = fgFeaturesStmt.executeQuery();
      List<FeaturegroupXAttrV2.SimpleFeatureDTO> features = new LinkedList<>();
      // regular features
      while (fgFeaturesResultSet.next()) {
        int fgId =  allFSCachedFResultSet.getInt(GET_HIVE_MANAGED_FEATUREGROUPS_S_ID);
        String featureName = fgFeaturesResultSet.getString(GET_FG_FEATURES_S_NAME);
        String featureDesc = fgFeaturesResultSet.getString(GET_FG_FEATURES_S_COMMENT);
        if(featureDesc == null) {
          featureDesc = "";
        }
        features.add(new FeaturegroupXAttrV2.SimpleFeatureDTO(featureName, featureDesc));
        insertCachedFeature(insertCachedFStmt, fgId, featureName, featureDesc);
      }
      return features;
    } finally {
      if(fgFeaturesStmt != null) {
        fgFeaturesStmt.close();
      }
    }
  }
  
  private List<FeaturegroupXAttrV2.SimpleFeatureDTO> getOnDemandFeaturesMigrate(ResultSet allFSOnDemandFGResultSet)
    throws SQLException {
    int fgId = allFSOnDemandFGResultSet.getInt(GET_ALL_ON_DEMAND_FGS_S_ID);
    PreparedStatement featuresStmt = null;
    try {
      featuresStmt = getOnDemandFGFeaturesStmt(fgId);
      ResultSet featuresResultSet = featuresStmt.executeQuery();
      List<FeaturegroupXAttrV2.SimpleFeatureDTO> features = new LinkedList<>();
      // regular features
      while (featuresResultSet.next()) {
        String featureName = featuresResultSet.getString(GET_ON_DEMAND_FG_FEATURES_S_NAME);
        String featureDesc = featuresResultSet.getString(GET_ON_DEMAND_FG_FEATURES_S_DESCRIPTION);
        if(featureDesc == null) {
          featureDesc = "";
        }
        features.add(new FeaturegroupXAttrV2.SimpleFeatureDTO(featureName, featureDesc));
      }
      return features;
    } finally {
      if(featuresStmt != null) {
        featuresStmt.close();
      }
    }
  }
  
  public void insertCachedFeature(PreparedStatement insertCachedFeaturesStmt, int id, String name, String desc)
    throws SQLException {
    insertCachedFeaturesStmt.setInt(INSERT_CACHED_FG_DESC_W_FG_ID, id);
    insertCachedFeaturesStmt.setString(INSERT_CACHED_FG_DESC_W_NAME, name);
    insertCachedFeaturesStmt.setString(INSERT_CACHED_FG_DESC_W_DESC, desc);
    insertCachedFeaturesStmt.addBatch();
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
  
  
  private JAXBContext jaxbContextMigrate() throws JAXBException {
    Map<String, Object> properties = new HashMap<>();
    properties.put(MarshallerProperties.JSON_INCLUDE_ROOT, false);
    properties.put(MarshallerProperties.MEDIA_TYPE, MediaType.APPLICATION_JSON);
    JAXBContext context = JAXBContextFactory.createContext(
      new Class[] {
        ProvCoreDTO.class,
        ProvTypeDTO.class,
        FeaturegroupXAttrV2.FullDTO.class,
        FeaturegroupXAttrV2.SimpleFeatureDTO.class
      },
      properties);
    return context;
  }
  
  private JAXBContext jaxbContextRollback() throws JAXBException {
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
  
  private String getFeaturegroupPath(String project, String featuregroup, int version) {
    return "/apps/hive/warehouse/" + project.toLowerCase() + "_featurestore.db/" + featuregroup + "_" + version;
  }
}
