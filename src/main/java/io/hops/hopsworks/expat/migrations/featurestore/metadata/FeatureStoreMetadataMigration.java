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

import com.google.common.base.Strings;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import io.hops.hopsworks.common.hdfs.DistributedFileSystemOps;
import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import io.hops.hopsworks.expat.db.DbConnectionFactory;
import io.hops.hopsworks.expat.migrations.MigrateStep;
import io.hops.hopsworks.expat.migrations.MigrationException;
import io.hops.hopsworks.expat.migrations.RollbackException;
import io.hops.hopsworks.expat.migrations.projects.util.HopsClient;
import io.hops.hopsworks.expat.migrations.projects.util.XAttrHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.Charsets;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.common.CheckedBiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class FeatureStoreMetadataMigration implements MigrateStep {
  private final static Logger LOGGER = LoggerFactory.getLogger(FeatureStoreMetadataMigration.class);
  
  private final static String GET_PROJECTS = "SELECT id, projectname from project";
  private final static int GET_PROJECTS_S_ID = 1;
  private final static int GET_PROJECTS_S_NAME = 2;
  private final static String GET_FS = "SELECT id, name FROM feature_store WHERE project_id=?";
  private final static int GET_FS_S_ID = 1;
  private final static int GET_FS_S_NAME = 2;
  private final static int GET_FS_W_PROJECT_ID = 1;
  private final static String GET_ARTIFACT_PART1 = "SELECT id, name, version FROM";
  private final static String GET_ARTIFACT_PART2 = "WHERE feature_store_id=?";
  private final static int GET_ARTIFACT_S_ID = 1;
  private final static int GET_ARTIFACT_S_NAME = 2;
  private final static int GET_ARTIFACT_S_VERSION = 3;
  private final static int GET_ARTIFACT_W_FS_ID = 1;
  private final static String GET_SCHEMAS = "SELECT id, name FROM feature_store_tag";
  private final static int GET_SCHEMAS_S_ID = 1;
  private final static int GET_SCHEMAS_S_NAME = 2;
  private final static String INSERT_FS_TAGS_PART1 = "INSERT INTO feature_store_tag_value(`schema_id`, `value`, `";
  private final static String INSERT_FS_TAGS_PART2 = "`) VALUE(?, ?, ?)";
  private final static int INSERT_FS_TAGS_V_SCHEMA_ID = 1;
  private final static int INSERT_FS_TAGS_V_VALUE = 2;
  private final static int INSERT_FS_TAGS_V_A_ID = 3;
  private final static String GET_TAGS_PART1 = "SELECT id, schema_id, value FROM feature_store_tag_value WHERE ";
  private final static String GET_TAGS_PART2 = "=?";
  private final static int GET_TAGS_S_ID = 1;
  private final static int GET_TAGS_S_SCHEMA_ID = 2;
  private final static int GET_TAGS_S_VALUE = 3;
  private final static int GET_TAGS_W_A_ID = 1;
  private final static String DELETE_TAGS = "DELETE FROM feature_store_tag_value WHERE id=?";
  private final static int DELETE_TAGS_W_ID = 1;
  private final static String INSERT_FS_KEYWORDS_PART1 = "INSERT INTO feature_store_keyword(`name`, `";
  private final static String INSERT_FS_KEYWORDS_PART2 = "`) VALUE(?, ?)";
  private final static int INSERT_FS_KEYWORDS_V_NAME = 1;
  private final static int INSERT_FS_KEYWORDS_V_A_ID = 2;
  private final static String GET_KEYWORDS_PART1 = "SELECT id, name FROM feature_store_keyword WHERE ";
  private final static String GET_KEYWORDS_PART2 = "=?";
  private final static int GET_KEYWORDS_S_ID = 1;
  private final static int GET_KEYWORDS_S_NAME = 2;
  private final static int GET_KEYWORDS_W_A_ID = 1;
  private final static String DELETE_KEYWORDS = "DELETE FROM feature_store_keyword WHERE id=?";
  private final static int DELETE_KEYWORDS_W_ID = 1;
  
  protected Connection connection = null;
  private String hopsUser;
  SimpleDateFormat formatter;
  boolean dryrun = false;
  DistributedFileSystemOps dfso = null;
  
  private void setup()
    throws ConfigurationException, SQLException {
    formatter = new SimpleDateFormat("yyyy-M-dd hh:mm:ss", Locale.ENGLISH);
    
    connection = DbConnectionFactory.getConnection();
    
    Configuration conf = ConfigurationBuilder.getConfiguration();
    hopsUser = conf.getString(ExpatConf.HOPS_CLIENT_USER);
    if (hopsUser == null) {
      throw new ConfigurationException(ExpatConf.HOPS_CLIENT_USER + " cannot be null");
    }
    dryrun = conf.getBoolean(ExpatConf.DRY_RUN);
  
    dfso = HopsClient.getDFSO(hopsUser);
  }
  
  private void close() throws SQLException {
    if(connection != null) {
      connection.close();
    }
  }
  
  @Override
  public void migrate() throws MigrationException {
    LOGGER.info("feature store tag migration");
    try {
      setup();
      connection.setAutoCommit(false);
      traverseElements(migrateTags(dryrun), migrateKeywords(dryrun));
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
    LOGGER.info("feature store tag rollback");
    try {
      setup();
      connection.setAutoCommit(false);
      traverseElements(rollbackTags(dryrun), rollbackKeywords(dryrun));
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
    String projectName;
    Integer fsId;
    String fsName;
    BiMap<String, Integer> schemas;
    String artifactType;
    ProcessState(String projectName, Integer fsId, String fsName, BiMap<String, Integer> schemas) {
      this.projectName = projectName;
      this.fsId = fsId;
      this.fsName = fsName;
      this.schemas = schemas;
    }
  
    ProcessState(ProcessState s, String artifactType) {
      this(s.projectName, s.fsId, s.fsName, s.schemas);
      this.artifactType = artifactType;
    }
    
    private ProcessState withArtifact(String artifactType) {
      return new ProcessState(this, artifactType);
    }
  }
  
  private void traverseElements(CheckedBiConsumer<ProcessState, ResultSet, Exception> tagAction,
                                CheckedBiConsumer<ProcessState, ResultSet, Exception> keywordAction)
    throws Exception {
    BiMap<String, Integer> schemas = HashBiMap.create();
    try(PreparedStatement schemasStmt = connection.prepareStatement(GET_SCHEMAS)) {
      ResultSet schemasResultSet = schemasStmt.executeQuery();
      while (schemasResultSet.next()) {
        int id = schemasResultSet.getInt(GET_SCHEMAS_S_ID);
        String name = schemasResultSet.getString(GET_SCHEMAS_S_NAME);
        schemas.put(name, id);
      }
    }
    try(PreparedStatement projStmt = connection.prepareStatement(GET_PROJECTS)) {
      ResultSet projResultSet = projStmt.executeQuery();
      while (projResultSet.next()) {
        Integer projectId = projResultSet.getInt(GET_PROJECTS_S_ID);
        String projectName = projResultSet.getString(GET_PROJECTS_S_NAME);
        try(PreparedStatement fsStmt = connection.prepareStatement(GET_FS)) {
          fsStmt.setInt(GET_FS_W_PROJECT_ID, projectId);
          ResultSet fsResultSet = fsStmt.executeQuery();
          while (fsResultSet.next()) {
            Integer fsId = fsResultSet.getInt(GET_FS_S_ID);
            String fsName = fsResultSet.getString(GET_FS_S_NAME);
            ProcessState state = new ProcessState(projectName, fsId, fsName, schemas);
            processArtifact(state.withArtifact("feature_group"), tagAction, keywordAction);
            processArtifact(state.withArtifact("feature_view"), tagAction, keywordAction);
            processArtifact(state.withArtifact("training_dataset"), tagAction, keywordAction);
          }
          fsResultSet.close();
        }
      }
      projResultSet.close();
      connection.commit();
    }
  }
  
  private String getArtifactStmt(String artifactType) {
    return GET_ARTIFACT_PART1 + " " + artifactType + " " + GET_ARTIFACT_PART2;
  }
  
  private CheckedBiConsumer<ProcessState, ResultSet, Exception> migrateTags(boolean dryRun) {
    return (ProcessState state, ResultSet artifact) -> {
      try {
        int id = artifact.getInt(GET_ARTIFACT_S_ID);
        String name = artifact.getString(GET_ARTIFACT_S_NAME);
        int version = artifact.getInt(GET_ARTIFACT_S_VERSION);
        String path = getArtifactPath(state.artifactType, state.projectName, state.fsName, name, version);
        Map<String, String> tags = readTags(XAttrHelper.getXAttr(dfso, path, "user", "tags"));
        for (Map.Entry<String, String> tag : tags.entrySet()) {
          Integer schemaId = state.schemas.get(tag.getKey());
          if (schemaId == null) {
            throw new IllegalStateException("schema not found:" + tag.getKey());
          }
          LOGGER.info("project:" + state.projectName + " " + state.artifactType
            + "<" + id + "," + name + "," + version + "> tag:" + tag.getKey());
          if(!dryRun) {
            try (PreparedStatement insertTagStmt = connection.prepareStatement(insertTagStmt(state.artifactType))) {
              insertTagStmt.setInt(INSERT_FS_TAGS_V_SCHEMA_ID, schemaId);
              insertTagStmt.setString(INSERT_FS_TAGS_V_VALUE, tag.getValue());
              insertTagStmt.setInt(INSERT_FS_TAGS_V_A_ID, id);
              insertTagStmt.executeUpdate();
            }
          }
        }
        if(!dryRun && !tags.isEmpty()) {
          XAttrHelper.deleteXAttr(dfso, path, "user", "tags");
        }
      } catch (SQLException e) {
        throw new MigrationException("error", e);
      }
    };
  }
  
  private CheckedBiConsumer<ProcessState, ResultSet, Exception> migrateKeywords(boolean dryRun) {
    return (ProcessState state, ResultSet artifact) -> {
      try {
        int id = artifact.getInt(GET_ARTIFACT_S_ID);
        String name = artifact.getString(GET_ARTIFACT_S_NAME);
        int version = artifact.getInt(GET_ARTIFACT_S_VERSION);
        String path = getArtifactPath(state.artifactType, state.projectName, state.fsName, name, version);
        Set<String> keywords = readKeywords(XAttrHelper.getXAttr(dfso, path, "user", "keywords"));
        for (String keyword : keywords) {
          LOGGER.info("project:" + state.projectName + " " + state.artifactType
            + "<" + id + "," + name + "," + version + "> keyword:" + keyword);
          if(!dryRun) {
            try (PreparedStatement insertTagStmt = connection.prepareStatement(insertKeywordStmt(state.artifactType))) {
              insertTagStmt.setString(INSERT_FS_KEYWORDS_V_NAME, keyword);
              insertTagStmt.setInt(INSERT_FS_KEYWORDS_V_A_ID, id);
              insertTagStmt.executeUpdate();
            }
          }
        }
        if(!dryRun && !keywords.isEmpty()) {
          XAttrHelper.deleteXAttr(dfso, path, "user", "keywords");
        }
      } catch (SQLException e) {
        throw new MigrationException("error", e);
      }
    };
  }
  
  private CheckedBiConsumer<ProcessState, ResultSet, Exception> rollbackTags(boolean dryRun) {
    return (ProcessState state, ResultSet artifact) -> {
      try {
        int id = artifact.getInt(GET_ARTIFACT_S_ID);
        String name = artifact.getString(GET_ARTIFACT_S_NAME);
        int version = artifact.getInt(GET_ARTIFACT_S_VERSION);
        String path = getArtifactPath(state.artifactType, state.projectName, state.fsName, name, version);
  
        BiMap<Integer, String> schemas = state.schemas.inverse();
        Map<String, String> tags = new HashMap<>();
        Set<Integer> toDelete = new HashSet<>();
        try (PreparedStatement stmt = connection.prepareStatement(getTagStmt(state.artifactType))) {
          stmt.setInt(GET_TAGS_W_A_ID, id);
          ResultSet resultSet = stmt.executeQuery();
          while (resultSet.next()) {
            int tagId = resultSet.getInt(GET_TAGS_S_ID);
            toDelete.add(tagId);
            int schemaId = resultSet.getInt(GET_TAGS_S_SCHEMA_ID);
            String tagValue = resultSet.getString(GET_TAGS_S_VALUE);
            String tagName = schemas.get(schemaId);
            if(tagName == null) {
              throw new MigrationException("missing schema:" + schemaId);
            }
            LOGGER.info("project:" + state.projectName + " " + state.artifactType +
              "<" + id + "," + name + "," + version + "> tag:" + tagName);
            tags.put(tagName, tagValue);
          }
          resultSet.close();
        }
        if(!dryRun && !tags.isEmpty()) {
          XAttrHelper.insertXAttr(dfso, path, "user", "tags", writeTags(tags));
          for (Integer tagId : toDelete) {
            try (PreparedStatement deleteTagStmt = connection.prepareStatement(DELETE_TAGS)) {
              deleteTagStmt.setInt(DELETE_TAGS_W_ID, tagId);
              deleteTagStmt.executeUpdate();
            }
          }
        }
      } catch (SQLException e) {
        throw new MigrationException("error", e);
      }
    };
  }
  
  private CheckedBiConsumer<ProcessState, ResultSet, Exception> rollbackKeywords(boolean dryRun) {
    return (ProcessState state, ResultSet artifact) -> {
      try {
        int id = artifact.getInt(GET_ARTIFACT_S_ID);
        String name = artifact.getString(GET_ARTIFACT_S_NAME);
        int version = artifact.getInt(GET_ARTIFACT_S_VERSION);
        String path = getArtifactPath(state.artifactType, state.projectName, state.fsName, name, version);
        
        Set<String> keywords = new HashSet<>();
        Set<Integer> toDelete = new HashSet<>();
        try (PreparedStatement stmt = connection.prepareStatement(getKeywordsStmt(state.artifactType))) {
          stmt.setInt(GET_KEYWORDS_W_A_ID, id);
          ResultSet resultSet = stmt.executeQuery();
          while (resultSet.next()) {
            int keywordId = resultSet.getInt(GET_KEYWORDS_S_ID);
            toDelete.add(keywordId);
            String keyword = resultSet.getString(GET_KEYWORDS_S_NAME);
            LOGGER.info("project:" + state.projectName + " " + state.artifactType +
              "<" + id + "," + name + "," + version + "> keyword:" + keyword);
            keywords.add(keyword);
          }
          resultSet.close();
        }
        if(!dryRun && !keywords.isEmpty()) {
          XAttrHelper.insertXAttr(dfso, path, "user", "keywords", writeKeywords(keywords));
          for (Integer keywordId : toDelete) {
            try (PreparedStatement deleteKeywordStmt = connection.prepareStatement(DELETE_KEYWORDS)) {
              deleteKeywordStmt.setInt(DELETE_KEYWORDS_W_ID, keywordId);
              deleteKeywordStmt.executeUpdate();
            }
          }
        }
      } catch (SQLException e) {
        throw new MigrationException("error", e);
      }
    };
  }
  
  private void processArtifact(ProcessState state,
                               CheckedBiConsumer<ProcessState, ResultSet, Exception> tagAction,
                               CheckedBiConsumer<ProcessState, ResultSet, Exception> keywordAction)
    throws Exception {
    try (PreparedStatement stmt = connection.prepareStatement(getArtifactStmt(state.artifactType))) {
      stmt.setInt(GET_ARTIFACT_W_FS_ID, state.fsId);
      ResultSet resultSet = stmt.executeQuery();
      while (resultSet.next()) {
        tagAction.accept(state, resultSet);
        keywordAction.accept(state, resultSet);
      }
      resultSet.close();
    }
  }
  
  private String getTagStmt(String artifactType) {
    return GET_TAGS_PART1 + artifactType + "_id" + GET_TAGS_PART2;
  }

  private static String insertTagStmt(String artifactType) {
    return INSERT_FS_TAGS_PART1 + artifactType + "_id" + INSERT_FS_TAGS_PART2;
  }
  
  private String getKeywordsStmt(String artifactType) {
    return GET_KEYWORDS_PART1 + artifactType + "_id" + GET_KEYWORDS_PART2;
  }
  
  private static String insertKeywordStmt(String artifactType) {
    return INSERT_FS_KEYWORDS_PART1 + artifactType + "_id" + INSERT_FS_KEYWORDS_PART2;
  }
  
  private Map<String, String> readTags(byte[] tags) {
    Map<String, String> tagsMap = new HashMap<>();
    if (tags == null) {
      return tagsMap;
    }
    String sTags = new String(tags, Charsets.UTF_8);
    if(!Strings.isNullOrEmpty(sTags)) {
      JSONArray tagsArr = new JSONArray(sTags);
      for (int i = 0; i < tagsArr.length(); i++) {
        JSONObject currentTag = tagsArr.getJSONObject(i);
        if(currentTag.has("value")) {
          tagsMap.put(currentTag.getString("key"), currentTag.getString("value"));
        } else {
          tagsMap.put(currentTag.getString("key"), "");
        }
      }
    }
    return tagsMap;
  }
  
  private byte[] writeTags(Map<String, String> tags) {
    List<JSONObject> tagsList = new ArrayList<>();
    for(Map.Entry<String, String> entry : tags.entrySet()) {
      JSONObject tag = new JSONObject();
      tag.put("key", entry.getKey());
      if(!Strings.isNullOrEmpty(entry.getValue())) {
        tag.put("value", entry.getValue());
      }
      tagsList.add(tag);
    }
    return new JSONArray(tagsList).toString().getBytes(Charsets.UTF_8);
  }
  
  private Set<String> readKeywords(byte[] keywords) {
    Set<String> keywordsSet = new HashSet<>();
    if (keywords == null) {
      return keywordsSet;
    }
    String sKeywords = new String(keywords, Charsets.UTF_8);
    if(!Strings.isNullOrEmpty(sKeywords)) {
      JSONArray keywordsArr = new JSONArray(sKeywords);
      for (int i = 0; i < keywordsArr.length(); i++) {
        String currentKeyword = keywordsArr.getString(i);
        keywordsSet.add(currentKeyword);
      }
    }
    return keywordsSet;
  }
  
  private byte[] writeKeywords(Set<String> keywords) {
    return new JSONArray(keywords).toString().getBytes(Charsets.UTF_8);
  }
  
  private String getArtifactPath(String artifactType, String projectName, String fsName, String name, Integer version){
    switch(artifactType) {
      case "feature_group": return getFGPath(fsName, name, version);
      case "feature_view": return getFVPath(projectName, name, version);
      case "training_dataset": return getTDPath(projectName, name, version);
      default: throw new IllegalArgumentException("unknown artifact type:" + artifactType);
    }
  }
  private String getFGPath(String fsName, String fgName, Integer fgVersion) {
    return "/apps/hive/warehouse/"
      + fsName + "_featurestore.db/"
      + fgName + "_" + fgVersion;
  }
  
  private String getFVPath(String projectName, String fvName, Integer fvVersion) {
    return "/Projects/" + projectName + "/"
      + projectName + "_Training_Datasets/"
      + ".featureviews/"
      + fvName + "_" + fvVersion;
  }
  
  private String getTDPath(String projectName, String tdName, Integer tdVersion) {
    return "/Projects/" + projectName + "/"
      + projectName + "_Training_Datasets/"
      + tdName + "_" + tdVersion;
  }
}
