package io.hops.hopsworks.expat.migrations.featurestore.featureview;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.hops.hopsworks.expat.migrations.MigrationException;
import io.hops.hopsworks.expat.migrations.RollbackException;
import io.hops.hopsworks.persistence.entity.featurestore.featuregroup.Featuregroup;
import io.hops.hopsworks.persistence.entity.featurestore.featureview.FeatureView;
import io.hops.hopsworks.persistence.entity.featurestore.featureview.ServingKey;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class SetServingKeys extends FeatureStoreMigration {

  private final static String GET_ALL_FEATURE_VIEWS =
      "SELECT fv.id, fv.feature_store_id, p.id AS pid, fv.name, fv.version " +
          "FROM feature_view AS fv " +
          "INNER JOIN feature_store AS fs ON fv.feature_store_id = fs.id " +
          "INNER JOIN project AS p ON fs.project_id = p.id";
  private final static String GET_TD_JOIN_CONDITION =
      "SELECT right_feature, left_feature FROM training_dataset_join_condition WHERE td_join = ?";
  private final static String GET_TD_JOIN =
      "SELECT id, idx, feature_group, prefix FROM training_dataset_join WHERE feature_view_id = ?";
  private final static String GET_LABEL_ONLY_FG =
      "SELECT feature_group FROM training_dataset_feature WHERE feature_view_id = ? AND label > 0";
  private final static String GET_PRIMARY_KEY =
      "SELECT cf.name AS cached_feature, sf.name AS stream_feature, odf.name AS on_demand_feature"
          + " FROM feature_group AS fg"
          + " LEFT JOIN on_demand_feature_group AS odfg ON fg.on_demand_feature_group_id = odfg.id"
          + " LEFT JOIN on_demand_feature AS odf ON odfg.id = odf.on_demand_feature_group_id"
          + " LEFT JOIN cached_feature_group AS cfg ON fg.cached_feature_group_id = cfg.id"
          + " LEFT JOIN cached_feature_extra_constraints AS cf ON cf.cached_feature_group_id = cfg.id"
          + " LEFT JOIN stream_feature_group AS sfg ON fg.stream_feature_group_id = sfg.id"
          + " LEFT JOIN cached_feature_extra_constraints AS sf ON sf.stream_feature_group_id = sfg.id"
          + " WHERE fg.id = ? AND (cf.primary_column > 0 OR sf.primary_column > 0 OR odf.primary_column > 0)";

  private final static String INSERT_SERVING_KEY =
      "INSERT INTO serving_key(" +
          // 1-5
          "`prefix`, `feature_name`, `join_on`, `join_index`, `feature_group_id`, " +
          // 6-10
          "`required`, `feature_view_id`" +
          ") " +
          "VALUE(" +
          "?, ?, ?, ?, ?, " +
          "?, ?" +
          ")";
  private final static String GET_SERVING_KEY = "SELECT * FROM serving_key";
  private final static String DELETE_SERVING_KEY = "DELETE FROM serving_key WHERE id = ?";

  @Override
  public void runMigration() throws MigrationException, SQLException {
    connection.setAutoCommit(false);
    PreparedStatement getFeatureViewsStatement = connection.prepareStatement(GET_ALL_FEATURE_VIEWS);
    ResultSet featureViews = getFeatureViewsStatement.executeQuery();
    PreparedStatement insertServingKeyStatement =
        connection.prepareStatement(INSERT_SERVING_KEY, Statement.RETURN_GENERATED_KEYS);
    int n = 0;
    int numServingKey = 0;
    int failedUpdate = 0;
    while (featureViews.next()) {
      n += 1;
      List<ServingKey> servingKeys = null;
      try {
        servingKeys = getServingKeyDTO(
            featureViews.getInt("pid"),
            featureViews.getInt("feature_store_id"),
            featureViews.getInt("id")
        );
        if (servingKeys.size() == 0) {
          LOGGER.warn(String.format("No serving keys will be added to fv `%s`.", featureViews.getString("name")));
          failedUpdate++;
          continue;
        }
      } catch (IOException e) {
        throw new MigrationException("Cannot get serving key from Hopsworks API.");
      }
      for (ServingKey servingKey : servingKeys) {
        setupServingKeyPreparedStatement(insertServingKeyStatement, featureViews.getInt("id"), servingKey);
        if (!dryRun) {
          int affectedRows = insertServingKeyStatement.executeUpdate();
          // need to commit here, otherwise tables are locked.
          connection.commit();
          if (affectedRows != 1) {
            throw new MigrationException("Creating serving key failed, no rows affected.");
          }
          numServingKey++;
        } else {
          insertServingKeyStatement.clearParameters();
          LOGGER.info(String.format("%d serving keys will be added to fv `%s`.", servingKeys.size(),
              featureViews.getString("name")));
        }
      }
    }

    getFeatureViewsStatement.close();
    insertServingKeyStatement.close();
    connection.commit();
    connection.setAutoCommit(true);
    if (!dryRun) {
      LOGGER.info(String.format("%d feature view have been updated with %d serving keys.", n, numServingKey));
      LOGGER.info(
          String.format("%d feature view have been updated successfully but %d feature view have failed to update.",
              n - failedUpdate, failedUpdate));
    } else {
      LOGGER.info(
          String.format("%d feature view will be updated successfully but %d feature view will be failed to update.",
              n - failedUpdate, failedUpdate));
    }

  }

  @Override
  public void runRollback() throws RollbackException {
    try {
      connection.setAutoCommit(false);
      PreparedStatement getServingKeyStatement = connection.prepareStatement(GET_SERVING_KEY);
      ResultSet servingKeys = getServingKeyStatement.executeQuery();
      PreparedStatement deleteServingKey =
          connection.prepareStatement(DELETE_SERVING_KEY);
      int n = 0;
      while (servingKeys.next()) {
        if (!dryRun) {
          deleteServingKey.setInt(1, servingKeys.getInt("id"));
          deleteServingKey.execute();
        }
        n++;
      }
      getServingKeyStatement.close();
      deleteServingKey.close();
      connection.commit();
      connection.setAutoCommit(true);
      if (dryRun) {
        LOGGER.info(n + " serving keys will be deleted.");
      } else {
        LOGGER.info(n + " serving keys have been deleted.");
      }
    } catch (SQLException e) {
      throw new RollbackException("Failed to remove serving keys.", e);
    }
  }

  static class TrainingDatasetJoin {
    private Integer index;
    private Integer featureGroupId;
    private String prefix;
    private List<TrainingDatasetJoinCondition> conditions;

    public TrainingDatasetJoin(Integer index, Integer featureGroupId, String prefix,
        List<TrainingDatasetJoinCondition> conditions) {
      this.index = index;
      this.featureGroupId = featureGroupId;
      this.prefix = prefix;
      this.conditions = conditions;
    }

    public Integer getIndex() {
      return index;
    }

    public Integer getFeatureGroupId() {
      return featureGroupId;
    }

    public String getPrefix() {
      return prefix;
    }

    public List<TrainingDatasetJoinCondition> getConditions() {
      return conditions;
    }

  }

  static class TrainingDatasetJoinCondition {
    private String rightFeature;
    private String leftFeature;

    public TrainingDatasetJoinCondition(String rightFeature, String leftFeature) {
      this.rightFeature = rightFeature;
      this.leftFeature = leftFeature;
    }

    public String getRightFeature() {
      return rightFeature;
    }

    public String getLeftFeature() {
      return leftFeature;
    }
  }

  private List<ServingKey> getServingKeyDTO(Integer projId, Integer fsId, Integer fvId)
      throws IOException, SQLException, MigrationException {
    List<ServingKey> servingKeys = Lists.newArrayList();
    Set<String> prefixFeatureNames = Sets.newHashSet();
    List<TrainingDatasetJoin> tdJoinsSorted =
        getTdJoin(fvId).stream().sorted(Comparator.comparingInt(TrainingDatasetJoin::getIndex)).collect(
            Collectors.toList());
    Optional<TrainingDatasetJoin> leftJoin =
        tdJoinsSorted.stream().filter(join -> join.getIndex().equals(0)).findFirst();
    // If a feature group is deleted, the corresponding join will be deleted by cascade.
    if (!leftJoin.isPresent()) {
      LOGGER.warn(
          "Cannot construct serving because some feature groups which are used in the query are removed. Fv id: "
              + fvId);
      return Lists.newArrayList();
    }
    Set<String> leftPrimaryKeys = getPrimaryKeys(leftJoin.get().getFeatureGroupId());
    for (TrainingDatasetJoin join : tdJoinsSorted) {
      // This contains join key and pk of feature group
      Set<String> tempPrefixFeatureNames = Sets.newHashSet();

      Set<String> primaryKeyNames = getPrimaryKeys(join.getFeatureGroupId());
      List<TrainingDatasetJoinCondition> joinConditions = join.getConditions() == null ?
          Lists.newArrayList() : join.getConditions();
      for (TrainingDatasetJoinCondition condition : joinConditions) {
        ServingKey servingKey = new ServingKey();
        String featureName = condition.getRightFeature();
        // Join on column, so ignore
        if (!primaryKeyNames.contains(featureName)) {
          continue;
        }
        servingKey.setFeatureName(featureName);
        String joinOn = condition.getLeftFeature();
        servingKey.setJoinOn(joinOn);
        // if join on (column of left fg) is not a primary key, set required to true
        servingKey.setRequired(!leftPrimaryKeys.contains(joinOn));
        Featuregroup fg = new Featuregroup(join.getFeatureGroupId());
        servingKey.setFeatureGroup(fg);
        FeatureView fv = new FeatureView();
        fv.setId(fvId);
        servingKey.setFeatureView(fv);
        servingKey.setJoinIndex(join.getIndex());
        // Set new prefix only if the key is required
        if (servingKey.getRequired()) {
          servingKey.setPrefix(
              getPrefixCheckCollision(prefixFeatureNames, servingKey.getFeatureName(), join.getPrefix()));
        } else {
          servingKey.setPrefix(join.getPrefix());
        }
        prefixFeatureNames.add(
            (servingKey.getPrefix() == null ? "" : servingKey.getPrefix()) + servingKey.getFeatureName());
        tempPrefixFeatureNames.add((join.getPrefix() == null ? "" : join.getPrefix()) + servingKey.getFeatureName());
        servingKeys.add(servingKey);
      }

      for (String pk : primaryKeyNames) {
        String prefixFeatureName = pk;
        if (!Strings.isNullOrEmpty(join.getPrefix())) {
          prefixFeatureName = join.getPrefix() + pk;
        }
        if (!tempPrefixFeatureNames.contains(prefixFeatureName)) {
          ServingKey servingKey = new ServingKey();
          servingKey.setFeatureName(pk);
          servingKey.setPrefix(getPrefixCheckCollision(prefixFeatureNames, pk, join.getPrefix()));
          servingKey.setRequired(true);
          Featuregroup fg = new Featuregroup(join.getFeatureGroupId());
          servingKey.setFeatureGroup(fg);
          servingKey.setJoinIndex(join.getIndex());
          FeatureView fv = new FeatureView();
          fv.setId(fvId);
          servingKeys.add(servingKey);
          prefixFeatureNames.add(
              (servingKey.getPrefix() == null ? "" : servingKey.getPrefix()) + servingKey.getFeatureName());
          tempPrefixFeatureNames.add(
              (join.getPrefix() == null ? "" : join.getPrefix()) + servingKey.getFeatureName());
        }
      }
    }
    Set<Integer> labelOnlyFgs = getLabelOnlyFeatureGroups(fvId);
    List<ServingKey> filteredServingKeys = Lists.newArrayList();
    for (ServingKey servingKey : servingKeys) {
      if (labelOnlyFgs.contains(servingKey.getFeatureGroup().getId())) {
        // Check if the serving key belongs to a left most fg by checking if the key was required by other fg.
        if (servingKeys.stream()
            .anyMatch(key -> ((servingKey.getPrefix() == null ? "" : servingKey.getPrefix())
                + servingKey.getFeatureName()).equals(key.getJoinOn()))) {
          filteredServingKeys.add(servingKey);
        }
      } else {
        filteredServingKeys.add(servingKey);
      }
    }
    return filteredServingKeys;
  }

  private List<TrainingDatasetJoin> getTdJoin(Integer fvId) throws SQLException {
    PreparedStatement getTdJoinStatement = connection.prepareStatement(GET_TD_JOIN);
    getTdJoinStatement.setInt(1, fvId);
    ResultSet tdJoinResultSet = getTdJoinStatement.executeQuery();

    List<TrainingDatasetJoin> tdJoins = Lists.newArrayList();
    while (tdJoinResultSet.next()) {
      Integer id = tdJoinResultSet.getInt("id");
      Integer index = tdJoinResultSet.getInt("idx");
      Integer fgId = tdJoinResultSet.getInt("feature_group");
      String prefix = tdJoinResultSet.getString("prefix");
      List<TrainingDatasetJoinCondition> tdConditions = getTdJoinCondition(id);
      tdJoins.add(new TrainingDatasetJoin(index, fgId, prefix, tdConditions));
    }
    return tdJoins;
  }

  private List<TrainingDatasetJoinCondition> getTdJoinCondition(Integer tdJoinId) throws SQLException {
    PreparedStatement getTdJoinConditionStatement = connection.prepareStatement(GET_TD_JOIN_CONDITION);
    getTdJoinConditionStatement.setInt(1, tdJoinId);
    ResultSet tdJoinConditionResultSet = getTdJoinConditionStatement.executeQuery();
    List<TrainingDatasetJoinCondition> tdJoinConditions = Lists.newArrayList();
    while (tdJoinConditionResultSet.next()) {
      tdJoinConditions.add(new TrainingDatasetJoinCondition(
          tdJoinConditionResultSet.getString("right_feature"),
          tdJoinConditionResultSet.getString("left_feature"))
      );
    }
    return tdJoinConditions;
  }

  private Set<String> getPrimaryKeys(Integer fgId) throws SQLException {
    PreparedStatement getPrimaryKeyStatement = connection.prepareStatement(GET_PRIMARY_KEY);
    getPrimaryKeyStatement.setInt(1, fgId);
    ResultSet primaryKeyResultSet = getPrimaryKeyStatement.executeQuery();
    Set<String> primaryKeys = Sets.newHashSet();
    while (primaryKeyResultSet.next()) {
      String cachedFeature = primaryKeyResultSet.getString("cached_feature");
      String onDemandFeature = primaryKeyResultSet.getString("on_demand_feature");
      String streamFeature = primaryKeyResultSet.getString("stream_feature");
      List<String> tempPk = Lists.newArrayList(cachedFeature, onDemandFeature, streamFeature).stream()
          .filter(f -> !Strings.isNullOrEmpty(f)).collect(Collectors.toList());
      if (tempPk.size() == 0) {
        LOGGER.warn("Cannot get primary key for feature group " + fgId);
      } else if (tempPk.size() > 1) {
        LOGGER.warn("Incorrect primary key for feature group " + fgId);
      }
      primaryKeys.addAll(tempPk);
    }
    return primaryKeys;
  }

  private Set<Integer> getLabelOnlyFeatureGroups(Integer fvId) throws SQLException {
    PreparedStatement getLabelOnlyFgStatement = connection.prepareStatement(GET_LABEL_ONLY_FG);
    getLabelOnlyFgStatement.setInt(1, fvId);
    ResultSet fgIdResultSet = getLabelOnlyFgStatement.executeQuery();
    Set<Integer> fgIds = Sets.newHashSet();
    while (fgIdResultSet.next()) {
      fgIds.add(fgIdResultSet.getInt("feature_group"));
    }
    return fgIds;
  }

  private String getPrefixCheckCollision(Set<String> prefixFeatureNames, String featureName, String prefix) {
    String prefixFeatureName = featureName;
    if (!Strings.isNullOrEmpty(prefix)) {
      prefixFeatureName = prefix + featureName;
    }
    if (prefixFeatureNames.contains(prefixFeatureName)) {
      // conflict with pk of other feature group, set new prefix
      String defaultPrefix;
      int i = 0;
      do {
        if (Strings.isNullOrEmpty(prefix)) {
          defaultPrefix = String.format("%d_", i);
        } else {
          defaultPrefix = String.format("%d_%s", i, prefix);
        }
        i++;
      } while (prefixFeatureNames.contains(defaultPrefix + featureName));
      return defaultPrefix;
    } else {
      return prefix;
    }
  }

  private void setupServingKeyPreparedStatement(PreparedStatement insertServingKeyStatement,
      int featureViewId, ServingKey servingKey) throws SQLException {
    insertServingKeyStatement.setString(1, servingKey.getPrefix());
    insertServingKeyStatement.setString(2, servingKey.getFeatureName());
    insertServingKeyStatement.setString(3, servingKey.getJoinOn());
    insertServingKeyStatement.setInt(4, servingKey.getJoinIndex());
    insertServingKeyStatement.setInt(5, servingKey.getFeatureGroup().getId());
    insertServingKeyStatement.setInt(6, servingKey.getRequired() ? 1 : 0);
    insertServingKeyStatement.setInt(7, featureViewId);
  }
}
