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
package io.hops.hopsworks.expat.migrations;

import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Utils {
  private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);
  
  public static final String ELASTIC_KAGENT_INDEX = "kagent";
  public static final String ELASTIC_KAGENT_INDEX_PATTERN = "_" + ELASTIC_KAGENT_INDEX + "-*";
  
  public static void createKibanaIndexPattern(String projectName,  String indexPattern, CloseableHttpClient httpClient,
      HttpHost kibana) throws IOException {
    createKibanaIndexPattern(projectName, indexPattern, httpClient, kibana,
        null);
  }
  
  public static void createKibanaIndexPattern(String projectName,  String indexPattern, CloseableHttpClient httpClient,
    HttpHost kibana, String token) throws IOException {
    String projectIndexPattern = projectName + indexPattern;
    String createIndexPatternStr = "/api/saved_objects/index-pattern/" + projectIndexPattern;
    String payload = "{\"attributes\": {\"title\": \"" + projectIndexPattern + "\"}}";
    
    CloseableHttpResponse response = null;
    try {
      HttpPost request = new HttpPost(createIndexPatternStr);
      request.setEntity(new StringEntity(payload));
      request.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
      request.addHeader("kbn-xsrf", "required");
      
      if(token != null){
        request.addHeader(HttpHeaders.AUTHORIZATION, token);
      }
      
      LOGGER.info("Creating index pattern: " + projectIndexPattern);
      response = httpClient.execute(kibana, request);
      String responseStr = EntityUtils.toString(response.getEntity());
      int status = response.getStatusLine().getStatusCode();
      if (status == 200) {
        LOGGER.info("Created index-pattern: " + projectIndexPattern);
      } else if (status == 409) {
        JSONObject jsonResponse = new JSONObject(responseStr);
        if (jsonResponse.getString("error").equals("Conflict")) {
          LOGGER.info("index-pattern " + projectIndexPattern + " already exists");
        }
      }
      LOGGER.debug(responseStr);
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }
  public static void deleteKibanaIndexPattern(String projectName, String indexPattern, CloseableHttpClient httpClient,
      HttpHost kibana) throws IOException {
    deleteKibanaIndexPattern(projectName, indexPattern, httpClient, kibana, null);
  }
  
  public static void deleteKibanaIndexPattern(String projectName, String indexPattern, CloseableHttpClient httpClient,
    HttpHost kibana, String token) throws IOException {
    String projectIndexPattern = projectName + indexPattern;
    String deleteIndexPatternPath = "/api/saved_objects/index-pattern/" + projectIndexPattern;
    
    CloseableHttpResponse response = null;
    try {
      HttpDelete request = new HttpDelete(deleteIndexPatternPath);
      request.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
      request.addHeader("kbn-xsrf", "required");
  
      if(token != null){
        request.addHeader(HttpHeaders.AUTHORIZATION, token);
      }
      
      LOGGER.info("Deleting index pattern: " + projectIndexPattern);
      response = httpClient.execute(kibana, request);
      int status = response.getStatusLine().getStatusCode();
      LOGGER.info("Return status: " + status);
      if (status == 200) {
        LOGGER.info("Deleted index pattern: " + projectIndexPattern);
      } else {
        LOGGER.info("Could not delete index pattern " + projectIndexPattern + " !!!");
      }
      if (LOGGER.isDebugEnabled()) {
        String responseStr = EntityUtils.toString(response.getEntity());
        LOGGER.debug(responseStr);
      }
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }
}
