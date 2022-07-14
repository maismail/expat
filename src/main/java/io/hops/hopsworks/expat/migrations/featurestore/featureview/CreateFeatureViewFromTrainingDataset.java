package io.hops.hopsworks.expat.migrations.featurestore.featureview;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.hops.hopsworks.common.featurestore.xattr.dto.FeatureViewXAttrDTO;
import io.hops.hopsworks.common.featurestore.xattr.dto.FeaturegroupXAttr;
import io.hops.hopsworks.common.featurestore.xattr.dto.FeaturestoreXAttrsConstants;
import io.hops.hopsworks.common.provenance.core.dto.ProvCoreDTO;
import io.hops.hopsworks.common.provenance.core.dto.ProvTypeDTO;
import io.hops.hopsworks.expat.db.dao.hdfs.inode.ExpatHdfsInode;
import io.hops.hopsworks.expat.migrations.MigrationException;
import io.hops.hopsworks.expat.migrations.RollbackException;
import io.hops.hopsworks.expat.migrations.projects.util.XAttrException;
import io.hops.hopsworks.expat.migrations.projects.util.XAttrHelper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.eclipse.persistence.jaxb.JAXBContextFactory;
import org.eclipse.persistence.jaxb.MarshallerProperties;
import org.eclipse.persistence.oxm.MediaType;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import java.io.IOException;
import java.io.StringWriter;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CreateFeatureViewFromTrainingDataset extends FeatureStoreMigration {

  private final static String GET_ALL_TRAINING_DATASETS =
      "SELECT a.id, a.feature_store_id, a.name, a.version, a.external_training_dataset_id, b.project_id, " +
          "d.uid, c.projectname, d.username, d.email, a.created, a.creator, a.description " +
          "FROM training_dataset AS a " +
          "INNER JOIN feature_store AS b ON a.feature_store_id = b.id " +
          "INNER JOIN project AS c ON b.project_id = c.id " +
          "INNER JOIN users AS d on a.creator = d.uid " +
          "WHERE a.feature_view_id is null";
  private final static String GET_ALL_FEATURE_VIEWS =
      "SELECT fv.id, p.projectname, fv.version " +
          "FROM feature_view AS fv " +
          "INNER JOIN feature_store AS fs ON fv.feature_store_id = fs.id " +
          "INNER JOIN project AS p ON fs.project_id = p.id";
  private final static String GET_FEATURES =
      "SELECT fg.name AS fg_name, fg.version AS fg_version, tdf.name AS name " +
          "FROM training_dataset_feature AS tdf " +
          "INNER JOIN feature_group AS fg ON tdf.feature_group = fg.id " +
          "WHERE %s";

  private final static String GET_ARRAY = "SELECT a.id FROM %s AS a WHERE %s";
  private final static String SET_FEATURE_VIEW = "UPDATE %s SET feature_view_id = ? WHERE %s = ?";
  private final static String REMOVE_FEATURE_VIEW_FROM_TABLES = "UPDATE %s SET %s = null WHERE %s = ?";
  private final static String DELETE_FEATURE_VIEW = "DELETE FROM feature_view WHERE id = ?";

  private final static String INSERT_FEATURE_VIEW =
      "INSERT INTO feature_view(" +
          // 1-5
          "`name`, `feature_store_id`, `created`, `creator`, `version`, " +
          // 6-10
          "`description`, `inode_pid`, `inode_name`, `partition_id`" +
          ") " +
          "VALUE(" +
          "?, ?, ?, ?, ?, " +
          "?, ?, ?, ?" +
          ")";
  private JAXBContext jaxbContext;

  public CreateFeatureViewFromTrainingDataset() throws JAXBException {
    super();
    jaxbContext = jaxbContextMigrate();
  }

  public void runMigration() throws MigrationException {
    // 1. Get all td
    // 2. Create fv from td
    // 3. set inode
    // 4. set xattr
    // 5. add fv to td, td feature, td join, td filter
    try {
      connection.setAutoCommit(false);
      PreparedStatement getTrainingDatasetsStatement = connection.prepareStatement(GET_ALL_TRAINING_DATASETS);
      PreparedStatement insertFeatureViewStatement =
          connection.prepareStatement(INSERT_FEATURE_VIEW, Statement.RETURN_GENERATED_KEYS);
      ResultSet trainingDatasets = getTrainingDatasetsStatement.executeQuery();
      Integer n = 0;
      while (trainingDatasets.next()) {
        n += 1;
        String name = trainingDatasets.getString("name");
        Integer featurestoreId = trainingDatasets.getInt("feature_store_id");
        Timestamp created = trainingDatasets.getTimestamp("created");
        Integer creator = trainingDatasets.getInt("creator");
        Integer version = trainingDatasets.getInt("version");
        String description = trainingDatasets.getString("description");

        Integer trainingDatasetId = trainingDatasets.getInt("id");
        String projectName = trainingDatasets.getString("projectname");
        String userName = trainingDatasets.getString("username");
        String email = trainingDatasets.getString("email");

        Integer[] features = getArray("training_dataset_feature", trainingDatasetId);

        insertFeatureViewStatement.setString(1, name);
        insertFeatureViewStatement.setInt(2, featurestoreId);
        insertFeatureViewStatement.setTimestamp(3, created);
        insertFeatureViewStatement.setInt(4, creator);
        insertFeatureViewStatement.setInt(5, version);
        insertFeatureViewStatement.setString(6, description);
        // inode
        setInode(insertFeatureViewStatement, projectName, userName, name, version);
        if (!dryRun) {
          int affectedRows = insertFeatureViewStatement.executeUpdate();
          // need to commit here, otherwise tables are locked.
          connection.commit();
          if (affectedRows != 1) {
            throw new MigrationException("Creating feature view failed, no rows affected.");
          } else {
            ResultSet generatedKeys = insertFeatureViewStatement.getGeneratedKeys();
            if (generatedKeys.next()) {
              Integer fvId = generatedKeys.getInt(1);
              setFeatureView("training_dataset_feature", "training_dataset", trainingDatasetId, fvId);
              setFeatureView("training_dataset_filter", "training_dataset_id", trainingDatasetId, fvId);
              setFeatureView("training_dataset_join", "training_dataset", trainingDatasetId, fvId);
              setFeatureView("training_dataset", "id", trainingDatasetId, fvId);
              connection.commit();
              setXAttr(featurestoreId, getFeatureViewFullPath(projectName, name, version).toString(),
                  description, created, email, features);
            } else {
              throw new MigrationException("Creating feature view failed, no ID obtained.");
            }
          }
        }
      }
      insertFeatureViewStatement.close();
      getTrainingDatasetsStatement.close();
      connection.commit();
      connection.setAutoCommit(true);
      LOGGER.info(n + " training dataset records have been updated.");
    } catch (SQLException e) {
      throw new MigrationException("Migration failed. Cannot commit.", e);
    } finally {
      super.close();
    }
  }

  public void runRollback() throws RollbackException {
    // 1. remove fv from td, td feature, td join, td filter
    // 2. remove file and inode
    // 3. remove fv
    try {
      Integer n = 0;
      connection.setAutoCommit(false);
      PreparedStatement getFeatureViewsStatement = connection.prepareStatement(GET_ALL_FEATURE_VIEWS);
      ResultSet featureViews = getFeatureViewsStatement.executeQuery();
      Set<String> projectNames = Sets.newHashSet();
      while (featureViews.next()) {
        n += 1;
        Integer fvId = featureViews.getInt("id");
        projectNames.add(featureViews.getString("projectname"));

        if (!dryRun) {
          removeFeatureViewFromTables(fvId);
          deleteFeatureView(fvId);
        }
      }
      // need to commit here so that removeFeatureViewFromTables works properly
      connection.commit();
      // Need to remove files separately because removing files remove all feature views and training datasets which
      // have not been rolled back yet.
      for (String projectName: projectNames) {
        removeFeatureViewFiles(projectName);
      }
      connection.setAutoCommit(true);
      LOGGER.info(n + " training dataset records have been rollback.");
    } catch (SQLException e) {
      throw new RollbackException("Rollback failed. Cannot commit.", e);
    } finally {
      super.close();
    }
  }

  private void removeFeatureViewFromTables(Integer fvId) throws RollbackException {
    removeFeatureViewFromTable("training_dataset_feature", fvId);
    removeFeatureViewFromTable("training_dataset_filter", fvId);
    removeFeatureViewFromTable("training_dataset_join", fvId);
    removeFeatureViewFromTable("training_dataset", fvId);
  }

  private void removeFeatureViewFromTable(String tableName, Integer featureViewId) throws RollbackException {
    String featureViewIdField = "feature_view_id";
    String sql = String.format(REMOVE_FEATURE_VIEW_FROM_TABLES, tableName, featureViewIdField, featureViewIdField);
    try {
      PreparedStatement statement = connection.prepareStatement(sql);
      statement.setInt(1, featureViewId);
      statement.execute();
      statement.close();
    } catch (SQLException e) {
      throw new RollbackException("Failed to remove featureDTOs.", e);
    }
  }

  private void removeFeatureViewFiles(String projectName) throws RollbackException {
    Path fvPath = getFeatureViewPath(projectName);
    Path fvPathHdfs = new Path("hdfs://" + fvPath);

    try {
      if (!dryRun && dfso.exists(fvPath)) {
        dfso.rm(fvPathHdfs, true);
      }
    } catch (IOException e) {
      throw new RollbackException("HDFS operation failed.", e);
    }
  }

  private void deleteFeatureView(Integer fvId) throws RollbackException {

    try {
      PreparedStatement statement = connection.prepareStatement(DELETE_FEATURE_VIEW);
      statement.setInt(1, fvId);
      statement.execute();
      statement.close();
    } catch (SQLException e) {
      throw new RollbackException("Failed to remove feature view.", e);
    }
  }

  private void setXAttr(Integer featurestoreId, String featureViewFullPath, String description,
      Date createDate, String email, Integer[] features)
      throws MigrationException {
    try {
      FeatureViewXAttrDTO fv = new FeatureViewXAttrDTO(featurestoreId,
          description,
          createDate,
          email,
          makeFeatures(featurestoreId, features));
      Marshaller marshaller = jaxbContext.createMarshaller();
      StringWriter sw = new StringWriter();
      marshaller.marshal(fv, sw);
      byte[] val = sw.toString().getBytes();
      if (val.length > 13500) {
        LOGGER.warn("xattr too large - skipping attaching features to featuregroup.");
        marshaller.marshal(fv, sw);
        val = sw.toString().getBytes();
      }
      if (!dryRun) {
        XAttrHelper.upsertProvXAttr(dfso, featureViewFullPath,
            FeaturestoreXAttrsConstants.FEATURESTORE, val);
      }
    } catch (JAXBException | XAttrException e) {
      throw new MigrationException("Cannot set attribute.", e);
    }
  }

  private String getFeaturesSql(Integer[] featureIds) {
    return String.format(GET_FEATURES,
        Joiner.on(" OR ").join(
            Stream.of(featureIds).map(feature -> "`tdf`.`id` = " + feature).collect(Collectors.toList())));
  }

  private List<FeaturegroupXAttr.SimplifiedDTO> makeFeatures(Integer featurestoreId, Integer[] featureIds)
      throws MigrationException {
    String getFeaturesSql = getFeaturesSql(featureIds);
    try {
      PreparedStatement getFeaturesStatement = connection.prepareStatement(getFeaturesSql);
      ResultSet features = getFeaturesStatement.executeQuery();
      Map<String, FeaturegroupXAttr.SimplifiedDTO> featureDtos = Maps.newHashMap();

      while (features.next()) {
        String featureGroupName = features.getString("fg_name");
        Integer featureGroupVersion = features.getInt("fg_version");
        String featureName = features.getString("name");
        String key = featurestoreId + featureGroupName + featureGroupVersion;
        if (featureDtos.containsKey(key)) {
          featureDtos.get(key).getFeatures().add(featureName);
        } else {
          featureDtos.put(key,
              new FeaturegroupXAttr.SimplifiedDTO(
                  featurestoreId,
                  featureGroupName,
                  featureGroupVersion
              ));
          featureDtos.get(key).setFeatures(Lists.newArrayList(featureName));
        }
      }
      getFeaturesStatement.close();
      return new ArrayList<>(featureDtos.values());
    } catch (SQLException e) {
      throw new MigrationException("Failed to make featureDTOs.", e);
    }
  }

  private JAXBContext jaxbContextMigrate() throws JAXBException {
    Map<String, Object> properties = new HashMap<>();
    properties.put(MarshallerProperties.JSON_INCLUDE_ROOT, false);
    properties.put(MarshallerProperties.MEDIA_TYPE, MediaType.APPLICATION_JSON);
    JAXBContext context = JAXBContextFactory.createContext(
        new Class[]{
            ProvCoreDTO.class,
            ProvTypeDTO.class,
            FeatureViewXAttrDTO.class,
            FeaturegroupXAttr.SimpleFeatureDTO.class
        },
        properties);
    return context;
  }

  private void setInode(PreparedStatement insertFeatureViewStatement, String projectName, String userName,
      String featureViewName, Integer featureViewVersion)
      throws SQLException, MigrationException {
    String owner = projectName + "__" + userName;
    String group = projectName + "__" + projectName + "_Training_Datasets";
    Path fvPath = getFeatureViewFullPath(projectName, featureViewName, featureViewVersion);
    Path fvPathHdfs = new Path("hdfs://" + fvPath);

    try {
      if (!dryRun) {
        if (!dfso.exists(fvPath)) {
          dfso.mkdirs(fvPathHdfs, FsPermission.getDefault());
          dfso.setOwner(fvPathHdfs, owner, group);
        }
        ExpatHdfsInode inode = inodeController.getInodeAtPath(fvPath.toString());
        insertFeatureViewStatement.setLong(7, inode.getParentId());
        insertFeatureViewStatement.setString(8, inode.getName());
        insertFeatureViewStatement.setLong(9, inode.getPartitionId());
      }
    } catch (IOException e) {
      throw new MigrationException("HDFS operation failed.", e);
    }
  }

  private Path getFeatureViewPath(String projectName) {
    String PATH_TO_FEATURE_VIEW = "/Projects/%s/%s_Training_Datasets" + Path.SEPARATOR + ".featureviews";
    return new Path(String.format(PATH_TO_FEATURE_VIEW, projectName, projectName));
  }

  private Path getFeatureViewFullPath(String projectName, String featureViewName, Integer featureViewVersion) {
    return new Path(getFeatureViewPath(projectName),
        featureViewName + "_" + featureViewVersion);
  }

  private void setFeatureView(String tableName, String idField, Integer trainingDatasetId, Integer featureViewId)
      throws MigrationException {
    String sql = String.format(SET_FEATURE_VIEW, tableName, idField);
    try {
      PreparedStatement setFeatureViewStatement = connection.prepareStatement(sql);
      setFeatureViewStatement.setInt(1, featureViewId);
      setFeatureViewStatement.setInt(2, trainingDatasetId);
      setFeatureViewStatement.execute();
      setFeatureViewStatement.close();
    } catch (SQLException e) {
      throw new MigrationException("Failed to make featureDTOs.", e);
    }
  }

  private Integer[] getArray(String tableName, Integer trainingDatasetId) throws MigrationException {
    String sql = String.format(GET_ARRAY, tableName, trainingDatasetId);
    try {
      PreparedStatement getFeaturesStatement = connection.prepareStatement(sql);
      ResultSet results = getFeaturesStatement.executeQuery();
      List<Integer> ids = Lists.newArrayList();
      while (results.next()) {
        ids.add(results.getInt("id"));
      }
      getFeaturesStatement.close();
      return ids.toArray(new Integer[0]);
    } catch (SQLException e) {
      throw new MigrationException("Failed to make featureDTOs.", e);
    }
  }
}
