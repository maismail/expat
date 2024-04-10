package io.hops.hopsworks.expat.migrations.featurestore.statistics;

/**
 * This file is part of Expat
 * Copyright (C) 2024, Hopsworks AB. All rights reserved
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

import io.hops.hopsworks.common.hdfs.DistributedFileSystemOps;
import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import io.hops.hopsworks.expat.db.DbConnectionFactory;
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

import javax.sql.rowset.serial.SerialBlob;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class StatisticsMigration implements MigrateStep {
  
  protected static final Logger LOGGER = LoggerFactory.getLogger(StatisticsMigration.class);
  
  protected Connection connection;
  protected DistributedFileSystemOps dfso = null;
  protected boolean dryRun;
  protected String hopsUser;
  
  protected ExpatInodeController inodeController;
  
  private static final String FEATURE_DESCRIPTIVE_STATISTICS_TABLE_NAME = "feature_descriptive_statistics";
  private static final String FEATURE_GROUP_DESCRIPTIVE_STATISTICS_TABLE_NAME = "feature_group_descriptive_statistics";
  private static final String FEATURE_GROUP_STATISTICS_TABLE_NAME = "feature_group_statistics";
  private static final String FEATURE_GROUP_COMMITS_TABLE_NAME = "feature_group_commit";
  private static final String TRAINING_DATASET_STATISTICS_TABLE_NAME = "training_dataset_statistics";
  private static final String TRAINING_DATASET_DESCRIPTIVE_STATISTICS_TABLE_NAME =
    "training_dataset_descriptive_statistics";
  private static final String TEST_DATASET_DESCRIPTIVE_STATISTICS_TABLE_NAME = "test_dataset_descriptive_statistics";
  private static final String VAL_DATASET_DESCRIPTIVE_STATISTICS_TABLE_NAME = "val_dataset_descriptive_statistics";
  private static final String FEATURE_STORE_ACTIVITY_TABLE_NAME = "feature_store_activity";
  
  private static final String SPLIT_NAME_TRAIN = "train";
  private static final String SPLIT_NAME_TEST = "test";
  private static final String SPLIT_NAME_VALIDATION = "validation";
  
  private static final String FOR_MIGRATION_FLAG = "for-migration";
  
  private final static String GET_FEATURE_DESCRIPTIVE_STATISTICS = String.format(
    "SELECT id, feature_type, count, num_non_null_values, num_null_values, extended_statistics_path FROM %s WHERE " +
      "feature_name = '%s'", FEATURE_DESCRIPTIVE_STATISTICS_TABLE_NAME, FOR_MIGRATION_FLAG);
  
  private final static String GET_EARLIEST_FG_COMMITS_PER_FEATURE_GROUP =
    String.format("SELECT feature_group_id, MIN(commit_id) from %s GROUP BY feature_group_id",
      FEATURE_GROUP_COMMITS_TABLE_NAME);
  
  private final static String INSERT_FEATURE_DESCRIPTIVE_STATISTICS = String.format(
    "INSERT INTO %s (feature_name, feature_type, count, completeness, num_non_null_values, num_null_values, " +
      "approx_num_distinct_values, min, max, sum, mean, stddev, percentiles, distinctness, entropy, uniqueness, " +
      "exact_num_distinct_values, extended_statistics_path) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
      "?, ?)", FEATURE_DESCRIPTIVE_STATISTICS_TABLE_NAME);
  
  private final static String INSERT_FEATURE_GROUP_DESCRIPTIVE_STATISTICS =
    String.format("INSERT INTO %s (feature_group_statistics_id, feature_descriptive_statistics_id) VALUES (?, ?)",
      FEATURE_GROUP_DESCRIPTIVE_STATISTICS_TABLE_NAME);
  
  private final static String INSERT_TRAINING_DATASET_DESCRIPTIVE_STATISTICS =
    String.format("INSERT INTO %s (training_dataset_statistics_id, feature_descriptive_statistics_id) VALUES (?, ?)",
      TRAINING_DATASET_DESCRIPTIVE_STATISTICS_TABLE_NAME);
  
  private final static String INSERT_TEST_DATASET_DESCRIPTIVE_STATISTICS =
    String.format("INSERT INTO %s (training_dataset_statistics_id, feature_descriptive_statistics_id) VALUES (?, ?)",
      TEST_DATASET_DESCRIPTIVE_STATISTICS_TABLE_NAME);
  
  private final static String INSERT_VAL_DATASET_DESCRIPTIVE_STATISTICS =
    String.format("INSERT INTO %s (training_dataset_statistics_id, feature_descriptive_statistics_id) VALUES (?, ?)",
      VAL_DATASET_DESCRIPTIVE_STATISTICS_TABLE_NAME);
  
  private final static String UPDATE_FEATURE_GROUP_DESCRIPTIVE_STATISTICS =
    String.format("UPDATE %s SET window_start_commit_time = ? WHERE id = ?", FEATURE_GROUP_STATISTICS_TABLE_NAME);
  
  private final static String DELETE_FEATURE_DESCRIPTIVE_STATISTICS =
    String.format("DELETE FROM %s WHERE id = ?", FEATURE_DESCRIPTIVE_STATISTICS_TABLE_NAME);
  
  private final static String DELETE_FEATURE_GROUP_STATISTICS =
    String.format("DELETE FROM %s WHERE id = ?", FEATURE_GROUP_STATISTICS_TABLE_NAME);
  
  private final static String DELETE_FEATURE_GROUP_STATISTICS_ACTIVITY =
    String.format("DELETE FROM %s WHERE feature_group_statistics_id = ?", FEATURE_STORE_ACTIVITY_TABLE_NAME);
  
  private final static String DELETE_TRAINING_DATASET_STATISTICS =
    String.format("DELETE FROM %s WHERE id = ?", TRAINING_DATASET_STATISTICS_TABLE_NAME);
  
  private final static String DELETE_TRAINING_DATASET_STATISTICS_ACTIVITY =
    String.format("DELETE FROM %s WHERE training_dataset_statistics_id = ?", FEATURE_STORE_ACTIVITY_TABLE_NAME);
  
  private final static String FEATURE_GROUP = "FEATURE_GROUP";
  private final static String TRAINING_DATASET = "TRAINING_DATASET";
  private Integer statisticsMigrationBatchSize;

  private class FeatureGroupStatisticsCommitWindow {
    private Integer fgStatisticsId;
    private Long windowStartCommitTime;

    private FeatureGroupStatisticsCommitWindow(Integer fgStatisticsId, Long windowStartCommitTime) {
      this.fgStatisticsId = fgStatisticsId;
      this.windowStartCommitTime = windowStartCommitTime;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      FeatureGroupStatisticsCommitWindow that = (FeatureGroupStatisticsCommitWindow) o;
      return Objects.equals(fgStatisticsId, that.fgStatisticsId)
          && Objects.equals(windowStartCommitTime, that.windowStartCommitTime);
    }

    @Override
    public int hashCode() {
      return Objects.hash(fgStatisticsId, windowStartCommitTime);
    }
  }

  public StatisticsMigration() {}
  
  @Override
  public void migrate() throws MigrationException {
    LOGGER.info("Starting migration of " + super.getClass().getName());
    
    try {
      setup();
      runMigration();
      close();
    } catch (ConfigurationException | SQLException ex) {
      String errorMsg = "Could not initialize database connection";
      LOGGER.error(errorMsg);
      close();
      throw new MigrationException(errorMsg, ex);
    } catch (IOException | IllegalAccessException | InstantiationException ex) {
      String errorMsg = "Could not migrate statistics";
      LOGGER.error(errorMsg);
      throw new MigrationException(errorMsg, ex);
    }
    LOGGER.info("Finished migration of " + super.getClass().getName());
  }
  
  @Override
  public void rollback() throws RollbackException {
    LOGGER.info("Starting rollback of " + super.getClass().getName());
    try {
      setup();
      runRollback();
      close();
    } catch (ConfigurationException | SQLException ex) {
      String errorMsg = "Could not initialize database connection";
      LOGGER.error(errorMsg);
      close();
      throw new RollbackException(errorMsg, ex);
    }
    LOGGER.info("Finished rollback of " + super.toString());
  }
  
  public void runMigration()
    throws MigrationException, SQLException, IOException, IllegalAccessException, InstantiationException {
    PreparedStatement fdsStmt = null;
    
    try {
      connection.setAutoCommit(false);
      
      // migrate feature descriptive statistics:
      // - insert statistics (per feature) into feature descriptive statistics table
      // - insert intermediate fg/td descriptive statistics rows
      // - update/remove statistics file. Only histograms, correlations, kll and unique values are kept in the file
      // - update start_commit_window in affected feature group statistic rows
      fdsStmt = connection.prepareStatement(GET_FEATURE_DESCRIPTIVE_STATISTICS);
      ResultSet fdsResultSet = fdsStmt.executeQuery();
      migrateFeatureDescriptiveStatistics(fdsResultSet);
      fdsStmt.close();
      
      // NOTE: The deletion of orphan fds statistics and files is delegated to the StatisticsCleaner.
      // There are two possible reasons why stats file are orphan during the migration:
      // - if multiple FG statistics on the same commit id, only the most recent is migrated, the rest become orphan.
      // - if multiple TD statistics on the same dataset, only the most recent is migrated, the rest become orphan.
      
      connection.commit();
      connection.setAutoCommit(true);
    } finally {
      if (fdsStmt != null) {
        fdsStmt.close();
      }
    }
  }
  
  private void migrateFeatureDescriptiveStatistics(ResultSet fdsResultSet)
    throws SQLException, MigrationException, IOException, IllegalAccessException, InstantiationException {
    List<Integer> fdsIds = new ArrayList<>(); // keep track of temporary fds to be removed
    HashMap<Integer, Long> fgsEarliestFgCommitIds; // <fg id, earliest fg commit id>
    boolean updateFgStatistics = false; // whether one or more fg stats need to be updated
    boolean deleteFgStatistics = false; // whether one or more fg stats need to be deleted
    boolean deleteTdStatistics = false; // whether one or more td stats need to be deleted
    
    // connections
    PreparedStatement insertFdsStmt = null;
    PreparedStatement insertFgFdsStmt = null;
    PreparedStatement updateFgsStmt = null;
    PreparedStatement deleteFgsStmt = null;
    PreparedStatement deleteFgsActStmt = null;
    PreparedStatement insertTrainDatasetFdsStmt = null;
    PreparedStatement insertTestDatasetFdsStmt = null;
    PreparedStatement insertValDatasetFdsStmt = null;
    PreparedStatement deleteTdsStmt = null;
    PreparedStatement deleteTdsActStmt = null;
    Set<Integer> deleteFGStatisticsIds = new HashSet<>();
    Set<Integer> deleteTDStatisticsIds = new HashSet<>();
    Set<FeatureGroupStatisticsCommitWindow> updatedFeatureGroupStatisticsCommitWindows = new HashSet<>();

    try {
      // earliest fg commit ids
      fgsEarliestFgCommitIds = getEarliestFgCommitIds();
      
      // fds connection
      insertFdsStmt = connection.prepareStatement(INSERT_FEATURE_DESCRIPTIVE_STATISTICS, new String[]{"id"});
      // table between fg stats and fds
      insertFgFdsStmt = connection.prepareStatement(INSERT_FEATURE_GROUP_DESCRIPTIVE_STATISTICS); // intermediate
      // fg stats connection
      updateFgsStmt = connection.prepareStatement(UPDATE_FEATURE_GROUP_DESCRIPTIVE_STATISTICS);
      deleteFgsStmt = connection.prepareStatement(DELETE_FEATURE_GROUP_STATISTICS);
      deleteFgsActStmt = connection.prepareStatement(DELETE_FEATURE_GROUP_STATISTICS_ACTIVITY);
      // td stats connections
      insertTrainDatasetFdsStmt = connection.prepareStatement(INSERT_TRAINING_DATASET_DESCRIPTIVE_STATISTICS);
      insertTestDatasetFdsStmt = connection.prepareStatement(INSERT_TEST_DATASET_DESCRIPTIVE_STATISTICS);
      insertValDatasetFdsStmt = connection.prepareStatement(INSERT_VAL_DATASET_DESCRIPTIVE_STATISTICS);
      deleteTdsStmt = connection.prepareStatement(DELETE_TRAINING_DATASET_STATISTICS);
      deleteTdsActStmt = connection.prepareStatement(DELETE_TRAINING_DATASET_STATISTICS_ACTIVITY);
      
      // per fds - migrate stats
      while (fdsResultSet.next()) {
        // extract fds column values
        int statisticsId = fdsResultSet.getInt(1); // this ID is the same for fg/td statistics and temporary fd stats
        String entityType = fdsResultSet.getString(2); // entity type is temp. stored in feature_type column
        int entityId = fdsResultSet.getInt(3); // fg id, used to look for earliest commit id, or td id
        long commitTime = fdsResultSet.getLong(4); // commit time is temp. stored in count column
        long windowEndCommitTime = fdsResultSet.getLong(5); // window end commit time
        String filePath = fdsResultSet.getString(6); // extended_stats_path contains the old stats file path
        
        LOGGER.info(
          String.format("[migrateFeatureDescriptiveStatistics] FdsResult: %s, %s, %s, %s, %s, %s", statisticsId,
            entityType, entityId, commitTime, windowEndCommitTime, filePath));
        
        fdsIds.add(statisticsId); // track temporary fds ids, to be removed after migration
        
        if (entityType.equals(FEATURE_GROUP)) {
          // get window start commit time
          Long windowStartCommitTime = fgsEarliestFgCommitIds.getOrDefault(entityId, null);
          LOGGER.info(String.format(
            "[migrateFeatureDescriptiveStatistics] -- window start commit is %s for feature group with id %s",
            windowStartCommitTime == null ? "null" : String.valueOf(windowStartCommitTime), entityId));
          if (windowStartCommitTime == null && windowEndCommitTime == 0) {
            windowEndCommitTime = commitTime; // for non-time-travel-enabled fgs, set end window as committime
          }
          // migrate fg stats
          boolean fdsInserted = migrateFeatureGroupStatistics(statisticsId, filePath, windowStartCommitTime,
            windowEndCommitTime, insertFdsStmt, insertFgFdsStmt);
          
          if (fdsInserted) {
            // set window start commit time if time travel-enabled fg
            if (loadFeatureGroupStatisticsCommitWindow(updatedFeatureGroupStatisticsCommitWindows, statisticsId,
                windowStartCommitTime)) {
              updateFgStatistics = true;
            }
          } else {  // if fds not inserted
            // this fg statistics could not be migrated to DB, so we delete the fg stats row.
            LOGGER.info(String.format(
              "[migrateFeatureDescriptiveStatistics] -- marking fg statistics for deletion, with id '%s' and " +
                "feature group id '%s'", statisticsId, entityId));
            deleteFGStatisticsIds.add(statisticsId);
            deleteFgStatistics = true;
          }
        } else if (entityType.equals(TRAINING_DATASET)) {
          boolean fdsInserted = migrateTrainingDatasetStatistics(statisticsId, filePath, commitTime, insertFdsStmt,
            insertTrainDatasetFdsStmt, insertTestDatasetFdsStmt, insertValDatasetFdsStmt);
          
          if (!fdsInserted) {   // if fds not inserted
            // this td statistics could not be migrated to DB, so we delete the td stats row.
            LOGGER.info(String.format(
              "[migrateFeatureDescriptiveStatistics] -- marking td statistics for deletion, with id '%s' and " +
                "training dataset id '%s'", statisticsId, entityId));
            deleteTDStatisticsIds.add(statisticsId);
            deleteTdStatistics = true;
          }
        } else {
          throw new MigrationException(
            "Unknown entity type: " + entityType + ". Expected values are " + FEATURE_GROUP + " or " +
              TRAINING_DATASET);
        }
      }
      
      // update feature group statistics window start commits
      if (updateFgStatistics) {
        if (dryRun) {
          LOGGER.info(String.format("[migrateFeatureDescriptiveStatistics] Update FGS: %s, size: %d",
                  updateFgsStmt.toString(), updatedFeatureGroupStatisticsCommitWindows.size()));
        } else {
          updateFeatureGroupStatisticsCommitWindow(updateFgsStmt, updatedFeatureGroupStatisticsCommitWindows);
        }
      }
      
      // delete feature group statistics that failed to be migrated
      if (deleteFgStatistics) {
        if (dryRun) {
          LOGGER.info(String.format("[migrateFeatureDescriptiveStatistics] Delete FGS: %s, size: %d",
              deleteFgsStmt.toString(), deleteFGStatisticsIds.size()));
          LOGGER.info(String.format("[migrateFeatureDescriptiveStatistics] Delete FGS ACTIVITY: %s",
            deleteFgsActStmt.toString()));
        } else {
          deleteStatisticsBatch(deleteFgsStmt, deleteFGStatisticsIds, "FGS");
        }
      }
      
      // delete training dataset statistics that failed to be migrated
      if (deleteTdStatistics) {
        if (dryRun) {
          LOGGER.info(String.format("[migrateFeatureDescriptiveStatistics] Delete TDS: %s, size: %s",
              deleteTdsStmt.toString(), deleteTDStatisticsIds.size()));
          LOGGER.info(String.format("[migrateFeatureDescriptiveStatistics] Delete TDS ACTIVITY: %s",
            deleteTdsActStmt.toString()));
        } else {
          deleteStatisticsBatch(deleteTdsStmt, deleteTDStatisticsIds, "TDS");
        }
      }

      // delete temporary feature descriptive statistics
      // NOTE: These feature descriptive statistics have become orphan. The deletion of orphan fds statistics and files
      // is delegated to the StatisticsCleaner.
      String fdsIdsStr = fdsIds.stream().map(String::valueOf).collect(Collectors.joining(", "));
      LOGGER.info(String.format("[deleteFeatureDescriptiveStatistics] Delegate deletion of FDS: %s, size: %d",
          fdsIdsStr, fdsIds.size()));
    } finally {
      if (insertFdsStmt != null) {
        insertFdsStmt.close();
      }
      if (insertFgFdsStmt != null) {
        insertFgFdsStmt.close();
      }
      if (updateFgsStmt != null) {
        updateFgsStmt.close();
      }
      if (deleteFgsStmt != null) {
        deleteFgsStmt.close();
      }
      if (deleteFgsActStmt != null) {
        deleteFgsActStmt.close();
      }
      if (insertTrainDatasetFdsStmt != null) {
        insertTrainDatasetFdsStmt.close();
      }
      if (insertTestDatasetFdsStmt != null) {
        insertTestDatasetFdsStmt.close();
      }
      if (insertValDatasetFdsStmt != null) {
        insertValDatasetFdsStmt.close();
      }
      if (deleteTdsStmt != null) {
        deleteTdsStmt.close();
      }
      if (deleteTdsActStmt != null) {
        deleteTdsActStmt.close();
      }
    }
  }
  
  private boolean migrateFeatureGroupStatistics(int statisticsId, String filePath, Long windowStartCommitTime,
    Long windowEndCommitTime, PreparedStatement insertFdsStmt, PreparedStatement insertIntermediateStmt)
    throws SQLException, IOException, MigrationException, IllegalAccessException, InstantiationException {
    // read and parse old hdfs file with statistics
    Collection<ExpatFeatureDescriptiveStatistics> fdsList = readAndParseLegacyStatistics(filePath);
    if (fdsList == null) {
      LOGGER.info(String.format("[migrateFeatureGroupStatistics] -- skipping fds row due to invalid " +
          "statistics file at '%s'", filePath));
      return false;  // skipping fds
    }
    
    // get stats file parent directory
    Path oldFilePath = new Path(filePath);
    Path parentDirPath = oldFilePath.getParent();
    
    // get owner, permissions and group
    FileStatus fileStatus = dfso.getFileStatus(oldFilePath);
    
    // insert feature descriptive statistics
    insertFeatureDescriptiveStatistics(statisticsId, fdsList, insertIntermediateStmt, insertFdsStmt,
      windowStartCommitTime, windowEndCommitTime, false, null, parentDirPath, fileStatus);
    
    // remove old hfds file
    if (dryRun) {
      LOGGER.info(String.format("[migrateFeatureGroupStatistics] Remove old hdfs stats file at: %s", filePath));
    } else {
      LOGGER.info(String.format("[migrateFeatureGroupStatistics] Remove old hdfs stats file at: %s", filePath));
      dfso.rm(filePath, false);
    }
    
    return true; // fds stored in the database
  }
  
  private boolean migrateTrainingDatasetStatistics(int statisticsId, String filePath, Long commitTime,
    PreparedStatement insertFdsStmt, PreparedStatement insertTrainDatasetFdsStmt,
    PreparedStatement insertTestDatasetFdsStmt, PreparedStatement insertValDatasetFdsStmt)
    throws SQLException, IOException, MigrationException, IllegalAccessException, InstantiationException {
    
    if (!dfso.exists(filePath)) {
      LOGGER.info("[migrateTrainingDatasetStatistics] statistics file does not exist: " + filePath);
      return false;
    }
    
    // get owner, permissions and group
    Path oldFilePath = new Path(filePath);
    FileStatus fileStatus = dfso.getFileStatus(oldFilePath);
    
    if (dfso.isDir(filePath)) { // training dataset with splits
      Path parentDirPath = oldFilePath;
      
      // train split stats
      String trainSplitFilePath = filePath + "/" + SPLIT_NAME_TRAIN + "_" + commitTime + ".json";
      Collection<ExpatFeatureDescriptiveStatistics> fdsList = readAndParseLegacyStatistics(trainSplitFilePath);
      if (fdsList == null) {
        LOGGER.info(String.format("[migrateTrainingDatasetStatistics] -- skipping fds row due to invalid " +
          "statistics file at '%s'", filePath));
        return false;  // skipping fds
      }
      // inserts fds and intermediate (train_fds) rows
      insertFeatureDescriptiveStatistics(statisticsId, fdsList, insertTrainDatasetFdsStmt, insertFdsStmt, null,
        commitTime, false, SPLIT_NAME_TRAIN, parentDirPath, fileStatus);
      if (dryRun) {
        LOGGER.info(
          String.format("[migrateTrainingDatasetStatistics] Remove old hdfs stats file at: %s", trainSplitFilePath));
      } else {
        LOGGER.info(
          String.format("[migrateTrainingDatasetStatistics] Remove old hdfs stats file at: %s", trainSplitFilePath));
        dfso.rm(trainSplitFilePath, false); // remove old hfds file
      }
      
      // test split stats
      String testSplitFilePath = filePath + "/" + SPLIT_NAME_TEST + "_" + commitTime + ".json";
      fdsList = readAndParseLegacyStatistics(testSplitFilePath);
      if (fdsList == null) {
        LOGGER.info(String.format("[migrateTrainingDatasetStatistics] -- skipping fds row due to invalid " +
          "statistics file at '%s'", filePath));
        return false;  // skipping fds
      }
      // inserts fds and intermediate (test_fds) rows
      insertFeatureDescriptiveStatistics(statisticsId, fdsList, insertTestDatasetFdsStmt, insertFdsStmt, null,
        commitTime, false, SPLIT_NAME_TEST, parentDirPath, fileStatus);
      if (dryRun) {
        LOGGER.info(
          String.format("[migrateTrainingDatasetStatistics] Remove old hdfs stats file at: %s", testSplitFilePath));
      } else {
        LOGGER.info(
          String.format("[migrateTrainingDatasetStatistics] Remove old hdfs stats file at: %s", testSplitFilePath));
        dfso.rm(testSplitFilePath, false); // remove old hfds file
      }
      
      // val split stats
      String valSplitFilePath = filePath + "/" + SPLIT_NAME_VALIDATION + "_" + commitTime + ".json";
      if (dfso.exists(valSplitFilePath)) {
        fdsList = readAndParseLegacyStatistics(valSplitFilePath);
        if (fdsList == null) {
          LOGGER.info(String.format("[migrateTrainingDatasetStatistics] -- skipping fds row due to invalid " +
            "statistics file at '%s'", filePath));
          return false;  // skipping fds
        }
        // inserts fds and intermediate (val_fds) rows
        insertFeatureDescriptiveStatistics(statisticsId, fdsList, insertValDatasetFdsStmt, insertFdsStmt, null,
          commitTime, false, SPLIT_NAME_VALIDATION, parentDirPath, fileStatus);
        if (dryRun) {
          LOGGER.info(
            String.format("[migrateTrainingDatasetStatistics] Remove old hdfs stats file at: %s", valSplitFilePath));
        } else {
          LOGGER.info(
            String.format("[migrateTrainingDatasetStatistics] Remove old hdfs stats file at: %s", valSplitFilePath));
          dfso.rm(valSplitFilePath, false); // remove old hfds file
        }
      }
    } else { // otherwise, either whole training dataset statistics or tr. functions statistics json file
      Collection<ExpatFeatureDescriptiveStatistics> fdsList = readAndParseLegacyStatistics(filePath);
      if (fdsList == null) {
        LOGGER.info(String.format("[migrateTrainingDatasetStatistics] -- skipping fds row due to invalid " +
          "statistics file at '%s'", filePath));
        return false;  // skipping fds
      }
      boolean beforeTransformation = filePath.contains("transformation_fn");
      Path parentDirPath = oldFilePath.getParent();
      // inserts fds and intermediate (train_fds) rows
      insertFeatureDescriptiveStatistics(statisticsId, fdsList, insertTrainDatasetFdsStmt, insertFdsStmt, null,
        commitTime, beforeTransformation, null, parentDirPath, fileStatus);
      if (dryRun) {
        LOGGER.info(String.format("[migrateTrainingDatasetStatistics] Remove old hdfs stats file at: %s", filePath));
      } else {
        LOGGER.info(String.format("[migrateTrainingDatasetStatistics] Remove old hdfs stats file at: %s", filePath));
        dfso.rm(filePath, false); // remove old hfds file
      }
    }
    
    return true;  // fds stored in the database
  }
  
  private HashMap<Integer, Long> getEarliestFgCommitIds() throws SQLException {
    PreparedStatement getEarliestFgCommitStmt = null;
    try {
      getEarliestFgCommitStmt = connection.prepareStatement(GET_EARLIEST_FG_COMMITS_PER_FEATURE_GROUP);
      ResultSet earliestFgCommits = getEarliestFgCommitStmt.executeQuery();
      
      HashMap<Integer, Long> fgsEarliestFgCommitIds = new HashMap<>();
      while (earliestFgCommits.next()) {
        fgsEarliestFgCommitIds.put(earliestFgCommits.getInt(1), earliestFgCommits.getLong(2));
      }
      
      LOGGER.info(
        String.format("[getEarliestFgCommitIds] list of earliest commits. Feature Groups: [%s], Commit IDs: [%s]",
          fgsEarliestFgCommitIds.keySet().stream().map(String::valueOf).collect(Collectors.joining(",")),
          fgsEarliestFgCommitIds.values().stream().map(String::valueOf).collect(Collectors.joining(","))));
      
      return fgsEarliestFgCommitIds;
    } finally {
      if (getEarliestFgCommitStmt != null) {
        getEarliestFgCommitStmt.close();
      }
    }
  }
  
  private Collection<ExpatFeatureDescriptiveStatistics> readAndParseLegacyStatistics(String filePath) {
    // read file content
    String fileContent;
    try {
      if (!dfso.exists(filePath)) {
        LOGGER.info(String.format("[readAndParseLegacyStatistics] statistics file does not exist at '%s'",filePath));
        return null; // no file content to parse
      }
      fileContent = dfso.cat(filePath);
      
      // parse feature descriptive statistics
      return ExpatFeatureDescriptiveStatistics.parseStatisticsJsonString(fileContent);
    } catch (IOException e) {
      LOGGER.info(String.format("[readAndParseLegacyStatistics] failed to read the file '%s' with error '%s'",
        filePath, e.getMessage()));
      return null; // no file content to parse
    }
  }
  
  private void insertFeatureDescriptiveStatistics(int statisticsId,
    Collection<ExpatFeatureDescriptiveStatistics> fdsList, PreparedStatement insertIntermediateStmt,
    PreparedStatement insertFdsStmt, Long windowStartCommitTime, Long windowEndCommitTime, boolean beforeTransformation,
    String splitName, Path dirPath, FileStatus fileStatus)
    throws SQLException, MigrationException, IOException, IllegalAccessException, InstantiationException {
    if (fdsList.isEmpty()) {
      return; // nothing to insert
    }
    
    // insert feature descriptive statistics rows
    for (ExpatFeatureDescriptiveStatistics fds : fdsList) {
      if(fds.featureName.length() <= 63) {
        // create extended statistics file, if needed
        fds.extendedStatistics =
            createExtendedStatisticsFile(windowStartCommitTime, windowEndCommitTime, fds.featureName,
                fds.extendedStatistics, beforeTransformation, splitName, dirPath, fileStatus);
        // set statement parameters
        setFdsStatementParameters(insertFdsStmt, fds);
        // add to batch
        insertFdsStmt.addBatch();
      } else {
        LOGGER.info(String.format("LONG FEATURE_NAME: %s - %s", fds.featureName, fds.id != null ? fds.id.toString() :
            "null"));
      }
    }
    
    if (dryRun) {
      LOGGER.info(
        String.format("[insertFeatureDescriptiveStatistics] Insert batch of FDS: %s", insertFdsStmt.toString()));
    } else {
      LOGGER.info(
        String.format("[insertFeatureDescriptiveStatistics] Insert batch of FDS: %s", insertFdsStmt.toString()));
      
      // insert fds
      insertFdsStmt.executeBatch();
      connection.commit();
      
      // insert intermediate table rows
      ResultSet generatedKeys = insertFdsStmt.getGeneratedKeys();
      while (generatedKeys.next()) {
        Integer fdsId = generatedKeys.getInt(1);
        insertIntermediateStmt.setInt(1, statisticsId);
        insertIntermediateStmt.setInt(2, fdsId);
        insertIntermediateStmt.addBatch();
      }
      insertIntermediateStmt.executeBatch();
      connection.commit();
    }
  }
  
  private void setFdsStatementParameters(PreparedStatement insertFdsStmt, ExpatFeatureDescriptiveStatistics fds)
    throws SQLException, MigrationException {
    setFdsPreparedStatementParameter(insertFdsStmt, 1, Types.VARCHAR, fds.featureName);
    setFdsPreparedStatementParameter(insertFdsStmt, 2, Types.VARCHAR, fds.featureType);
    setFdsPreparedStatementParameter(insertFdsStmt, 3, Types.BIGINT, fds.count);
    setFdsPreparedStatementParameter(insertFdsStmt, 4, Types.FLOAT, fds.completeness);
    setFdsPreparedStatementParameter(insertFdsStmt, 5, Types.BIGINT, fds.numNonNullValues);
    setFdsPreparedStatementParameter(insertFdsStmt, 6, Types.BIGINT, fds.numNullValues);
    setFdsPreparedStatementParameter(insertFdsStmt, 7, Types.BIGINT, fds.approxNumDistinctValues);
    setFdsPreparedStatementParameter(insertFdsStmt, 8, Types.FLOAT, fds.min);
    setFdsPreparedStatementParameter(insertFdsStmt, 9, Types.FLOAT, fds.max);
    setFdsPreparedStatementParameter(insertFdsStmt, 10, Types.FLOAT, fds.sum);
    setFdsPreparedStatementParameter(insertFdsStmt, 11, Types.FLOAT, fds.mean);
    setFdsPreparedStatementParameter(insertFdsStmt, 12, Types.FLOAT, fds.stddev);
    setFdsPreparedStatementParameter(insertFdsStmt, 13, Types.BLOB, convertPercentilesToBlob(fds.percentiles));
    setFdsPreparedStatementParameter(insertFdsStmt, 14, Types.FLOAT, fds.distinctness);
    setFdsPreparedStatementParameter(insertFdsStmt, 15, Types.FLOAT, fds.entropy);
    setFdsPreparedStatementParameter(insertFdsStmt, 16, Types.FLOAT, fds.uniqueness);
    setFdsPreparedStatementParameter(insertFdsStmt, 17, Types.BIGINT, fds.exactNumDistinctValues);
    setFdsPreparedStatementParameter(insertFdsStmt, 18, Types.VARCHAR, fds.extendedStatistics);
  }
  
  private void setFdsPreparedStatementParameter(PreparedStatement stmt, int paramPosition, int sqlType, Object value)
    throws SQLException, MigrationException {
    if (value == null) {
      stmt.setNull(paramPosition, sqlType);
      return;
    }
    switch (sqlType) {
      case Types.BIGINT:
        stmt.setLong(paramPosition, (Long) value);
        break;
      case Types.VARCHAR:
        stmt.setString(paramPosition, (String) value);
        break;
      case Types.BLOB:
        stmt.setBlob(paramPosition, (Blob) value);
        break;
      case Types.FLOAT:
        stmt.setDouble(paramPosition, (Double) value);
        break;
      case Types.INTEGER:
        stmt.setInt(paramPosition, (Integer) value);
        break;
      default:
        throw new MigrationException("Unknown sql type '" + String.valueOf(sqlType) + "' for feature descriptive " +
          "statistics parameter in position: " + String.valueOf(paramPosition));
    }
  }

  private Blob convertPercentilesToBlob(List<Double> percentiles) throws SQLException {
    return percentiles != null && !percentiles.isEmpty() ? new SerialBlob(convertPercentilesToByteArray(percentiles)) :
      null;
  }
  
  private String createExtendedStatisticsFile(Long windowStartCommitTime, Long windowEndCommitTime, String featureName,
    String extendedStatistics, Boolean beforeTransformation, String splitName, Path dirPath, FileStatus fileStatus)
    throws IOException, MigrationException, SQLException, IllegalAccessException, InstantiationException {
    if (extendedStatistics == null || extendedStatistics.isEmpty()) {
      return null; // no extended stats to persist
    }
    // Persist extended statistics on a file in hopsfs
    Path filePath;
    if (beforeTransformation) {
      filePath =
        new Path(dirPath, transformationFnStatisticsFileName(windowStartCommitTime, windowEndCommitTime, featureName));
    } else {
      if (splitName != null) {
        filePath = new Path(dirPath, splitStatisticsFileName(splitName, windowEndCommitTime, featureName));
      } else {
        filePath = new Path(dirPath, statisticsFileName(windowStartCommitTime, windowEndCommitTime, featureName));
      }
    }
    // get owner, permissions and group
    String owner = fileStatus.getOwner();
    FsPermission permissions = fileStatus.getPermission();
    String group = fileStatus.getGroup();
    if (dryRun) {
      LOGGER.info(String.format(
        "[createExtendedStatisticsFile] Create FDS hdfs file at: %s with owner: %s, group: %s and content: %s",
        filePath, owner, group, "extendedStatistics"));
    } else {
      LOGGER.info(String.format(
        "[createExtendedStatisticsFile] Create FDS hdfs file at: %s with owner: %s, group: %s and content: %s",
        filePath, owner, group, "extendedStatistics"));
      // create file
      dfso.create(filePath, extendedStatistics);
      setOwnershipAndPermissions(filePath, owner, permissions, group, dfso);
    }
    return filePath.toString();
  }
  
  private String transformationFnStatisticsFileName(Long startCommitTime, Long endCommitTime, String featureName) {
    String name = "transformation_fn_";
    name += startCommitTime != null ? startCommitTime + "_" : "";
    return name + endCommitTime + "_" + featureName + ".json";
  }
  
  private String splitStatisticsFileName(String splitName, Long commitTime, String featureName) {
    return commitTime + "_" + splitName + "_" + featureName + ".json";
  }
  
  private String statisticsFileName(Long startCommitTime, Long endCommitTime, String featureName) {
    String name = startCommitTime != null ? startCommitTime + "_" : "";
    return name + endCommitTime + "_" + featureName + ".json";
  }
  
  private boolean loadFeatureGroupStatisticsCommitWindow(Set<FeatureGroupStatisticsCommitWindow> loadList,
      Integer fgStatisticsId, Long windowStartCommitTime) throws SQLException {
    if (windowStartCommitTime == null) {
      return false; // skip update, no time travel-enabled feature group
    }
    loadList.add(new FeatureGroupStatisticsCommitWindow(fgStatisticsId, windowStartCommitTime));
    return true;
  }

  private void updateFeatureGroupStatisticsCommitWindow(PreparedStatement updateFgsStmt,
      Set<FeatureGroupStatisticsCommitWindow> listToUpdate) throws SQLException {
    int c = 1;
    for (FeatureGroupStatisticsCommitWindow e : listToUpdate) {
      updateFgsStmt.setLong(1, e.windowStartCommitTime);
      updateFgsStmt.setInt(2, e.fgStatisticsId);
      updateFgsStmt.addBatch();
      if (c % statisticsMigrationBatchSize == 0) {
        LOGGER.info(String.format("[migrateFeatureDescriptiveStatistics] Update FGS: %s", updateFgsStmt.toString()));
        updateFgsStmt.executeBatch();
        connection.commit();
      }
      c++;
    }
    LOGGER.info(String.format("[migrateFeatureDescriptiveStatistics] Update FGS: %s", updateFgsStmt.toString()));
    updateFgsStmt.executeBatch();
    connection.commit();
  }
  
  private void deleteStatisticsBatch(PreparedStatement deleteStatisticsStmt, Set<Integer> statisticsIdsToDelete,
      String log)
      throws SQLException {
    int c = 1;
    for (Integer id : statisticsIdsToDelete) {
      deleteStatisticsStmt.setInt(1, id);
      deleteStatisticsStmt.addBatch();
      if (c % statisticsMigrationBatchSize == 0) {
        LOGGER.info(String.format("[migrateFeatureDescriptiveStatistics] Delete %s: %s", log,
            deleteStatisticsStmt.toString()));
        deleteStatisticsStmt.executeBatch();
        connection.commit();
      }
      c++;
    }
    LOGGER.info(String.format("[migrateFeatureDescriptiveStatistics] Delete %s: %s", log,
        deleteStatisticsStmt.toString()));
    deleteStatisticsStmt.executeBatch();
    connection.commit();
  }
  
  private byte[] convertPercentilesToByteArray(List<Double> percentilesList) {
    if (percentilesList == null) {
      return null;
    }
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
         ObjectOutputStream oos = new ObjectOutputStream(bos)) {
      oos.writeObject(percentilesList);
      oos.flush();
      return bos.toByteArray();
    } catch (IOException e) {
      LOGGER.info("Cannot convert percentiles map to byte array");
    }
    return null;
  }
  
  private void setOwnershipAndPermissions(Path filePath, String username, FsPermission permissions, String group,
    DistributedFileSystemOps dfso) throws IOException {
    dfso.setOwner(filePath, username, group);
    if (permissions != null) {
      dfso.setPermission(filePath, permissions);
    }
  }
  
  public void runRollback() throws RollbackException {
  
  }
  
  protected void setup() throws ConfigurationException, SQLException {
    connection = DbConnectionFactory.getConnection();
    
    Configuration conf = ConfigurationBuilder.getConfiguration();
    hopsUser = conf.getString(ExpatConf.HOPS_CLIENT_USER);
    if (hopsUser == null) {
      throw new ConfigurationException(ExpatConf.HOPS_CLIENT_USER + " cannot be null");
    }
    dfso = HopsClient.getDFSO(hopsUser);
    dryRun = conf.getBoolean(ExpatConf.DRY_RUN);
    inodeController = new ExpatInodeController(this.connection);
    this.statisticsMigrationBatchSize = Integer.parseInt(System.getProperty("statisticsmigrationbatch", "100"));
    LOGGER.info("Statistics migration batch size: " + statisticsMigrationBatchSize);
  }
  
  protected void close() {
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException ex) {
        LOGGER.error("failed to close jdbc connection", ex);
      }
    }
    if (dfso != null) {
      dfso.close();
    }
  }
}
