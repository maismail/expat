/*
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
package io.hops.hopsworks.expat.migrations.elk;

import io.hops.hopsworks.common.util.Settings;
import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import io.hops.hopsworks.expat.db.DbConnectionFactory;
import io.hops.hopsworks.expat.migrations.MigrateStep;
import io.hops.hopsworks.expat.migrations.MigrationException;
import io.hops.hopsworks.expat.migrations.RollbackException;
import io.hops.hopsworks.expat.migrations.Utils;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * Adds the project specific index-patterns to the .kibana
 * elasticsearch index associated with that project.
 */
public class MultiTenantKibana implements MigrateStep {
  
  private static final Logger LOGGER = LogManager.getLogger(MultiTenantKibana.class);
  
  private static final String GET_PROJECT_IDS_AND_NAMES = "SELECT id," +
      "projectname FROM project";
  
  private Connection connection;
  private CloseableHttpClient httpClient;
  private HttpHost kibana;
  private HttpHost hopsworks;
  private String serviceJwt;
  
  private void setup() throws ConfigurationException, SQLException,
      GeneralSecurityException {
    connection = DbConnectionFactory.getConnection();
    Configuration conf = ConfigurationBuilder.getConfiguration();
    String kibanaURI = conf.getString(ExpatConf.KIBANA_URI);
    String hopsworksURI = conf.getString(ExpatConf.HOPSWORKS_URL);
    serviceJwt = conf.getString(ExpatConf.HOPSWORKS_SERVICE_JWT);
    
    if (kibanaURI == null) {
      throw new ConfigurationException(ExpatConf.KIBANA_URI + " cannot be null");
    }
    
    if (hopsworksURI == null) {
      throw new ConfigurationException(ExpatConf.HOPSWORKS_URL + " cannot be null");
    }
  
    if (serviceJwt == null) {
      throw new ConfigurationException(ExpatConf.HOPSWORKS_SERVICE_JWT + " cannot be null");
    }
    
    kibana = HttpHost.create(kibanaURI);
    hopsworks = HttpHost.create(hopsworksURI);
    
    httpClient = HttpClients
        .custom()
        .setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(
            CookieSpecs.IGNORE_COOKIES).build())
        .setSSLContext(new SSLContextBuilder().loadTrustMaterial((x509Certificates, s) -> true).build())
        .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
        .build();
  }
  
  
  @Override
  public void migrate() throws MigrationException {
    try {
      setup();
      Map<Integer, String> projects = getProjects();
    
      for (Integer project : projects.keySet()) {
        String projectName = projects.get(project);
        String elkToken = getELKToken(project);
        
        String[] indexPatterns = {Settings.ELASTIC_LOGS_INDEX_PATTERN,
            Settings.ELASTIC_KAGENT_INDEX_PATTERN,
            Settings.ELASTIC_SERVING_INDEX_PATTERN,
            Settings.ELASTIC_BEAMJOBSERVER_INDEX_PATTERN,
            Settings.ELASTIC_BEAMSDKWORKER_INDEX_PATTERN};
        
        for(String indexPattern : indexPatterns){
          try {
            Utils.createKibanaIndexPattern(projectName, indexPattern,
                httpClient, kibana, elkToken);
          } catch (IOException ex) {
            LOGGER.error("Ooops could not create index-pattern " + indexPattern + " for " +
                projectName + " Moving on...", ex);
          }
        }
      }
    } catch (Exception ex) {
      LOGGER.error(ex.getMessage(), ex);
      throw new MigrationException("Error in migration step " + MultiTenantKibana.class.getSimpleName(), ex);
    }
  }
  
  @Override
  public void rollback() throws RollbackException {
    try {
      setup();
      Map<Integer, String> projects = getProjects();
    
      for (Integer project : projects.keySet()) {
        String projectName = projects.get(project);
        String elkToken = getELKToken(project);
      
        String[] indexPatterns = {Settings.ELASTIC_LOGS_INDEX_PATTERN,
            Settings.ELASTIC_KAGENT_INDEX_PATTERN,
            Settings.ELASTIC_SERVING_INDEX_PATTERN,
            Settings.ELASTIC_BEAMJOBSERVER_INDEX_PATTERN,
            Settings.ELASTIC_BEAMSDKWORKER_INDEX_PATTERN};
      
        for(String indexPattern : indexPatterns){
          try {
            Utils.deleteKibanaIndexPattern(projectName, indexPattern,
                httpClient, kibana, elkToken);
          } catch (IOException ex) {
            LOGGER.error("Ooops could not delete index-pattern for " + projectName + " Moving on...", ex);
          }
        }
      }
    } catch (Exception ex) {
      LOGGER.error(ex.getMessage(), ex);
      throw new RollbackException("Error in rollback step " + MultiTenantKibana.class.getSimpleName(), ex);
    }
  }
  
  private Map<Integer, String> getProjects() throws SQLException {
    Map<Integer, String> projects = new HashMap<>();
    ResultSet projectsRS = null;
    Statement projectsStmt = connection.createStatement();
    try {
      projectsRS = projectsStmt.executeQuery(GET_PROJECT_IDS_AND_NAMES);
      
      while (projectsRS.next()) {
        Integer projectId = projectsRS.getInt("id");
        String projectName = projectsRS.getString("projectname");
        if (projectId != null && projectName != null && !projectName.isEmpty()) {
          LOGGER.debug("Found project " + projectId + " : " + projectName);
          projects.put(projectId, projectName);
        }
      }
      return projects;
    } finally {
      if (projectsRS != null) {
        projectsRS.close();
      }
      if (projectsStmt != null) {
        projectsStmt.close();
      }
    }
  }
  
  private String getELKToken(Integer projectId) throws IOException {
    String url = "/hopsworks-api/api/jwt/elk/token/" + projectId;
    HttpGet request = new HttpGet(url);
    request.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
    request.addHeader(HttpHeaders.AUTHORIZATION, "Bearer " + serviceJwt);
    
    HttpResponse response = httpClient.execute(hopsworks, request);
    String responseStr = EntityUtils.toString(response.getEntity());
    JSONObject jsonResponse =
        new JSONObject(responseStr);
    String token = jsonResponse.getString("token");
    return "Bearer " + token;
  }
}
