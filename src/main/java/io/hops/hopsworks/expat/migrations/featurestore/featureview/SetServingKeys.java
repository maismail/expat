package io.hops.hopsworks.expat.migrations.featurestore.featureview;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import io.hops.hopsworks.common.featurestore.featureview.ServingKeyDTO;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import io.hops.hopsworks.expat.migrations.MigrationException;
import io.hops.hopsworks.expat.migrations.RollbackException;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

public class SetServingKeys extends FeatureStoreMigration {

  private CloseableHttpClient httpClient;
  private static PoolingHttpClientConnectionManager httpConnectionManager;
  private ObjectMapper objectMapper = new ObjectMapper();
  private HttpHost fv;
  private String serviceJwt;

  private final static String GET_ALL_FEATURE_VIEWS =
      "SELECT fv.id, fv.feature_store_id, p.id AS pid, fv.name, fv.version " +
          "FROM feature_view AS fv " +
          "INNER JOIN feature_store AS fs ON fv.feature_store_id = fs.id " +
          "INNER JOIN project AS p ON fs.project_id = p.id";
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
    String hopsworksURI = conf.getString(ExpatConf.HOPSWORKS_URL);
    if (hopsworksURI == null) {
      throw new MigrationException(ExpatConf.HOPSWORKS_URL + " cannot be null");
    }
    serviceJwt = conf.getString(ExpatConf.HOPSWORKS_SERVICE_JWT);
    if (serviceJwt == null) {
      throw new MigrationException(ExpatConf.HOPSWORKS_SERVICE_JWT + " cannot be null");
    }
    fv = HttpHost.create(hopsworksURI);
    connection.setAutoCommit(false);
    httpConnectionManager = new PoolingHttpClientConnectionManager();
    httpConnectionManager.setDefaultMaxPerRoute(5);
    httpClient = HttpClients.custom().setConnectionManager(httpConnectionManager).build();
    PreparedStatement getFeatureViewsStatement = connection.prepareStatement(GET_ALL_FEATURE_VIEWS);
    ResultSet featureViews = getFeatureViewsStatement.executeQuery();
    PreparedStatement insertServingKeyStatement =
        connection.prepareStatement(INSERT_SERVING_KEY, Statement.RETURN_GENERATED_KEYS);
    int n = 0;
    int numServingKey = 0;
    int failedUpdate = 0;
    while (featureViews.next()) {
      n += 1;
      List<ServingKeyDTO> servingKeyDTOS = null;
      try {
        servingKeyDTOS = getServingKeyDTO(
            featureViews.getInt("pid"),
            featureViews.getInt("feature_store_id"),
            featureViews.getString("name"),
            featureViews.getInt("version")
        );
        if (servingKeyDTOS.size() == 0) {
          LOGGER.warn(String.format("No serving keys will be added to fv `%s`.", featureViews.getString("name")));
          failedUpdate++;
          continue;
        }
      } catch (IOException e) {
        throw new MigrationException("Cannot get serving key from Hopsworks API.");
      }
      for (ServingKeyDTO dto : servingKeyDTOS) {
        setupServingKeyPreparedStatement(insertServingKeyStatement, featureViews.getInt("id"), dto);
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
          LOGGER.info(String.format("%d serving keys will be added to fv `%s`.", servingKeyDTOS.size(),
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
    }
    LOGGER.info(String.format("%d feature view will be updated but %d feature view will be failed to update.",
        n - failedUpdate, failedUpdate));
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

  private List<ServingKeyDTO> getServingKeyDTO(Integer projId, Integer fsId, String fvName, Integer fvVersion)
      throws IOException, MigrationException {
    CloseableHttpResponse response = null;
    try {
      HttpGet request = new HttpGet(String.format(
          "/hopsworks-api/api/project/%d/featurestores/%d/featureview/%s/version/%d/servingKeys",
          projId, fsId, fvName, fvVersion));
      request.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
      request.addHeader(HttpHeaders.AUTHORIZATION, "Bearer " + serviceJwt);
      response = httpClient.execute(fv, request);
      int status = response.getStatusLine().getStatusCode();
      if (status == 200) {
        return Arrays.asList(objectMapper.readValue(EntityUtils.toString(response.getEntity()), ServingKeyDTO[].class));
      } else {
        String entity = EntityUtils.toString(response.getEntity());
        if (entity.contains("Featuregroup wasn't found.")) {
          return Lists.newArrayList();
        } else {
          throw new MigrationException(String.format("Cannot get serving key from fv name: %s version: %d: %s",
              fvName, fvVersion, entity));
        }
      }
    } catch (IOException e) {
      throw new MigrationException("Cannot get serving key from Hopsworks API: " + e.getMessage());
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }

  private void setupServingKeyPreparedStatement(PreparedStatement insertServingKeyStatement,
      int featureViewId, ServingKeyDTO servingKeyDTO) throws SQLException {
    insertServingKeyStatement.setString(1, servingKeyDTO.getPrefix());
    insertServingKeyStatement.setString(2, servingKeyDTO.getFeatureName());
    insertServingKeyStatement.setString(3, servingKeyDTO.getJoinOn());
    insertServingKeyStatement.setInt(4, servingKeyDTO.getJoinIndex());
    insertServingKeyStatement.setInt(5, servingKeyDTO.getFeatureGroup().getId());
    insertServingKeyStatement.setInt(6, servingKeyDTO.getRequired() ? 1 : 0);
    insertServingKeyStatement.setInt(7, featureViewId);
  }
}
