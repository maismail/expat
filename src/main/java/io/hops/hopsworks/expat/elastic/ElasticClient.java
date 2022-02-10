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
package io.hops.hopsworks.expat.elastic;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;

public class ElasticClient {
  private final static Logger LOGGER = LogManager.getLogger(ElasticClient.class);
  
  public static void deleteProvenanceProjectIndex(CloseableHttpClient httpClient, HttpHost elastic, Long projectIId,
                                                  String elasticUser, String elasticPass) throws IOException {
    deleteIndex(httpClient, elastic, elasticUser, elasticPass, projectIId + "__file_prov");
  }
  
  public static void deleteAppProvenanceIndex(CloseableHttpClient httpClient, HttpHost elastic,
                                              String elasticUser, String elasticPass) throws IOException {
    deleteIndex(httpClient, elastic, elasticUser, elasticPass, "app_prov");
  }
  
  public static void deleteTemplate(CloseableHttpClient httpClient, HttpHost elastic,
                                    String elasticUser, String elasticPass, String name) throws IOException {
    CloseableHttpResponse response = null;
    try {
      HttpDelete request = new HttpDelete("_template/" + name);
      String encodedAuth = Base64.getEncoder().encodeToString((elasticUser + ":" + elasticPass).getBytes());
      request.addHeader(HttpHeaders.AUTHORIZATION, "Basic " + encodedAuth);
      LOGGER.info("Deleting index template:{}", name);
      response = httpClient.execute(elastic, request);
      JSONObject jsonResponse = new JSONObject(EntityUtils.toString(response.getEntity()));
      int status = response.getStatusLine().getStatusCode();
      if (status == 200) {
        LOGGER.info("Deleted index template:{}", name);
      } else if (status == 404) {
        LOGGER.info("Index template:{} already deleted", name);
      } else {
        LOGGER.info(jsonResponse);
        if (!jsonResponse.getJSONObject("error").get("reason").toString().startsWith("no such index template")) {
          throw new IllegalStateException("Could not delete index template:" + name);
        }
        LOGGER.info("Skipping index template:{} - already deleted", name);
      }
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }
  
  public static void deleteIndex(CloseableHttpClient httpClient, HttpHost elastic,
                                 String elasticUser, String elasticPass, String index) throws IOException {
    CloseableHttpResponse response = null;
    try {
      HttpDelete request = new HttpDelete(index);
      request.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
      String encodedAuth = Base64.getEncoder().encodeToString((elasticUser + ":" + elasticPass).getBytes());
      request.addHeader(HttpHeaders.AUTHORIZATION, "Basic " + encodedAuth);
      LOGGER.info("Deleting index:{}", index);
      response = httpClient.execute(elastic, request);
      JSONObject jsonResponse = new JSONObject(EntityUtils.toString(response.getEntity()));
      int status = response.getStatusLine().getStatusCode();
      if (status == 200) {
        LOGGER.info("Deleted index:{}", index);
      } else {
        if (!jsonResponse.getJSONObject("error").get("reason").toString().startsWith("no such index")) {
          throw new IllegalStateException("Could not delete index:" + index);
        }
        LOGGER.info("Skipping index:{} - already deleted", index);
      }
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }
  
  public static void reindex(CloseableHttpClient httpClient, HttpHost elastic, String elasticUser, String elasticPass,
                             String fromIndex, String toIndex)
      throws IOException, URISyntaxException {
    reindex(httpClient, elastic, elasticUser, elasticPass, fromIndex, toIndex, Optional.empty());
  }
  
  public static void reindex(CloseableHttpClient httpClient, HttpHost elastic, String elasticUser, String elasticPass,
                            String fromIndex, String toIndex, String script)
      throws IOException, URISyntaxException {
    reindex(httpClient, elastic, elasticUser, elasticPass, fromIndex, toIndex, Optional.of(script));
  }
  
  public static void reindex(CloseableHttpClient httpClient, HttpHost elastic, String elasticUser, String elasticPass,
                             String fromIndex, String toIndex, Optional<String> script)
    throws IOException, URISyntaxException {
    CloseableHttpResponse response = null;
    try {
      URIBuilder uriBuilder = new URIBuilder();
      uriBuilder
        .setPathSegments("_reindex")
        .setParameter("wait_for_completion", "true")
        .setParameter("refresh", "true");
      HttpPost request = new HttpPost(uriBuilder.build());
      request.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
      String encodedAuth = Base64.getEncoder().encodeToString((elasticUser + ":" + elasticPass).getBytes());
      request.addHeader(HttpHeaders.AUTHORIZATION, "Basic " + encodedAuth);
      String requestBody = getReindexBody(fromIndex, toIndex, script);
      HttpEntity entity = new ByteArrayEntity(requestBody.getBytes(StandardCharsets.UTF_8));
      request.setEntity(entity);
      
      LOGGER.info("Reindexing from:{} to:{}", fromIndex, toIndex);
      response = httpClient.execute(elastic, request);
      JSONObject jsonResponse = new JSONObject(EntityUtils.toString(response.getEntity()));
      LOGGER.info("Response:{}", jsonResponse);
      int status = response.getStatusLine().getStatusCode();
      if (status == 200) {
        if(jsonResponse.getJSONArray("failures").length() != 0) {
          throw new IllegalStateException("failed to reindex:" + jsonResponse.getJSONArray("failures"));
        }
      } else {
        if (jsonResponse.getJSONObject("error").get("reason").toString().startsWith("no such index")) {
          throw new IllegalStateException("Could not reindex - indices do not exist");
        } else {
          throw new IllegalStateException("Could not reindex:" + jsonResponse.getJSONObject("error").get("reason"));
        }
      }
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }
  
  private static String getReindexBody(String fromIndex, String toIndex, Optional<String> script) {
    JsonObject bodyJson = new JsonObject();
    JsonObject sourceJson = new JsonObject();
    bodyJson.add("source", sourceJson);
    sourceJson.addProperty("index", fromIndex);
    
    JsonObject destJson = new JsonObject();
    bodyJson.add("dest", destJson);
    destJson.addProperty("index", toIndex);
    
    if(script.isPresent()) {
      JsonObject scriptJson = new JsonObject();
      bodyJson.add("script", scriptJson);
      scriptJson.addProperty("lang", "painless");
      scriptJson.addProperty("source", script.get());
    }
    
    return new GsonBuilder().create().toJson(bodyJson);
  }
  
  public static void createTemplate(CloseableHttpClient httpClient, HttpHost elastic,
                                    String elasticUser, String elasticPass,
                                    String name, String mapping, String indexPattern)
    throws IOException {
    CloseableHttpResponse response = null;
    try {
      HttpPut request = new HttpPut("_template/" + name);
      request.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
      String encodedAuth = Base64.getEncoder().encodeToString((elasticUser + ":" + elasticPass).getBytes());
      request.addHeader(HttpHeaders.AUTHORIZATION, "Basic " + encodedAuth);
      String body = "{\"index_patterns\": [\"" + indexPattern + "\"]," + mapping + "}";
      HttpEntity entity = new ByteArrayEntity(body.getBytes(StandardCharsets.UTF_8));
      request.setEntity(entity);
      LOGGER.info("Creating index template:{}", name);
      response = httpClient.execute(elastic, request);
      JSONObject jsonResponse = new JSONObject(EntityUtils.toString(response.getEntity()));
      int status = response.getStatusLine().getStatusCode();
      if (status == 200) {
        LOGGER.info("Created index template:{}", name);
      } else {
        throw new IllegalStateException("Could not create index template - unknown elastic error:"
          + jsonResponse.getJSONObject("error").get("reason"));
      }
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }
  
  public static void createIndex(CloseableHttpClient httpClient, HttpHost elastic,
                                 String elasticUser, String elasticPass, String index)
      throws IOException {
    createIndex(httpClient, elastic, elasticUser, elasticPass, index, Optional.empty());
  }

  public static void createIndex(CloseableHttpClient httpClient, HttpHost elastic,
                                 String elasticUser, String elasticPass, String index, Optional<String> mapping)
      throws IOException {
    CloseableHttpResponse response = null;
    try {
      HttpPut request = new HttpPut(index);
      request.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
      String encodedAuth = Base64.getEncoder().encodeToString((elasticUser + ":" + elasticPass).getBytes());
      request.addHeader(HttpHeaders.AUTHORIZATION, "Basic " + encodedAuth);
      if(mapping.isPresent()) {
        HttpEntity entity = new ByteArrayEntity(mapping.get().getBytes(StandardCharsets.UTF_8));
        request.setEntity(entity);
      }
      LOGGER.info("Creating index:{}", index);
      response = httpClient.execute(elastic, request);
      JSONObject jsonResponse = new JSONObject(EntityUtils.toString(response.getEntity()));
      int status = response.getStatusLine().getStatusCode();
      if (status == 200) {
        LOGGER.info("Created index:{}", index);
      } else {
        throw new IllegalStateException("Could not create index - unknown elastic error:"
          + jsonResponse.getJSONObject("error").get("reason"));
      }
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }
  
  public static boolean indexExists(CloseableHttpClient httpClient, HttpHost elastic,
    String elasticUser, String elasticPass, String index) throws IOException {
    CloseableHttpResponse response = null;
    try {
      HttpHead request = new HttpHead(index);
      request.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
      String encodedAuth = Base64.getEncoder().encodeToString((elasticUser + ":" + elasticPass).getBytes());
      request.addHeader(HttpHeaders.AUTHORIZATION, "Basic " + encodedAuth);
      LOGGER.info("Checking index:{}", index);
      response = httpClient.execute(elastic, request);
      int status = response.getStatusLine().getStatusCode();
      if (status == 200) {
        LOGGER.info("Checked index:{}", index);
        return true;
      } else if(status == 404) {
        LOGGER.info("Index:{} does not exist", index);
        return false;
      } else {
        String errorMsg = "Could not check index existence - ";
        if(response.getStatusLine().getReasonPhrase() != null) {
          errorMsg += response.getStatusLine().getReasonPhrase();
        } else {
          errorMsg += "unknown exception";
        }
        throw new IllegalStateException(errorMsg);
      }
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }
  
  public static Integer itemCount(CloseableHttpClient httpClient, HttpHost elastic,
    String elasticUser, String elasticPass, String index) throws IOException, URISyntaxException {
    CloseableHttpResponse response = null;
    try {
      URIBuilder builder = new URIBuilder();
      builder
        .setPath(index + "/_search")
        .setParameter("size", "0");
      HttpGet request = new HttpGet(builder.build());
      request.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
      String encodedAuth = Base64.getEncoder().encodeToString((elasticUser + ":" + elasticPass).getBytes());
      request.addHeader(HttpHeaders.AUTHORIZATION, "Basic " + encodedAuth);
      LOGGER.info("Item count index:{}", index);
      response = httpClient.execute(elastic, request);
      JSONObject jsonResponse = new JSONObject(EntityUtils.toString(response.getEntity()));
      int status = response.getStatusLine().getStatusCode();
      if (status == 200) {
        int count = jsonResponse.getJSONObject("hits").getJSONObject("total").getInt("value");
        LOGGER.info("Item count index:{} item count:{}", index, count);
        return count;
      } else {
        throw new IllegalStateException("Could not check index count - unknown elastic error:"
          + jsonResponse.getJSONObject("error").get("reason"));
      }
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }
  
  
  public static void createKibanaIndexPattern(String indexPattern,
      CloseableHttpClient httpClient,
      HttpHost kibana, String elasticUser, String elasticPass,
      String timeFieldName) throws IOException {
    String createIndexPatternStr =
        "/api/saved_objects/index-pattern/" + indexPattern;
    String payload = "{\"attributes\": {\"title\": \"" + indexPattern
        + "\", \"timeFieldName\": \"" + timeFieldName + "\"}}";
    
    CloseableHttpResponse response = null;
    try {
      HttpPost request = new HttpPost(createIndexPatternStr);
      request.setEntity(new StringEntity(payload));
      request.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
      request.addHeader("kbn-xsrf", "required");
      
      String encodedAuth = Base64.getEncoder()
          .encodeToString((elasticUser + ":" + elasticPass).getBytes());
      request.addHeader(HttpHeaders.AUTHORIZATION, "Basic " + encodedAuth);
      
      LOGGER.info("Creating index pattern: " + indexPattern);
      response = httpClient.execute(kibana, request);
      String responseStr = EntityUtils.toString(response.getEntity());
      int status = response.getStatusLine().getStatusCode();
      if (status == 200) {
        LOGGER.info("Created index-pattern: " + indexPattern);
      } else if (status == 409) {
        JSONObject jsonResponse = new JSONObject(responseStr);
        if (jsonResponse.getString("error").equals("Conflict")) {
          LOGGER.info("index-pattern " + indexPattern + " already exists");
        }
      }
      LOGGER.debug(responseStr);
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }
  
  public static void deleteKibanaIndexPattern(String indexPattern,
      CloseableHttpClient httpClient,
      HttpHost kibana, String elasticUser, String elasticPass)
      throws IOException {
    String deleteIndexPatternPath =
        "/api/saved_objects/index-pattern/" + indexPattern;
    
    CloseableHttpResponse response = null;
    try {
      HttpDelete request = new HttpDelete(deleteIndexPatternPath);
      request.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
      request.addHeader("kbn-xsrf", "required");
      
      String encodedAuth = Base64.getEncoder()
          .encodeToString((elasticUser + ":" + elasticPass).getBytes());
      request.addHeader(HttpHeaders.AUTHORIZATION, "Basic " + encodedAuth);
      
      LOGGER.info("Deleting index pattern: " + indexPattern);
      response = httpClient.execute(kibana, request);
      int status = response.getStatusLine().getStatusCode();
      LOGGER.info("Return status: " + status);
      if (status == 200) {
        LOGGER.info("Deleted index pattern: " + indexPattern);
      } else {
        LOGGER.info("Could not delete index pattern " + indexPattern + " !!!");
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
  
  public static void createSnapshotRepo(CloseableHttpClient httpClient, HttpHost elastic, String elasticUser,
                                        String elasticPass, String repoName, String repoLocation)
    throws IOException, URISyntaxException {
    CloseableHttpResponse response = null;
    try {
      URIBuilder uriBuilder = new URIBuilder()
        .setPathSegments("_snapshot", repoName);
      HttpPut request = new HttpPut(uriBuilder.build());
      request.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
      String encodedAuth = Base64.getEncoder().encodeToString((elasticUser + ":" + elasticPass).getBytes());
      request.addHeader(HttpHeaders.AUTHORIZATION, "Basic " + encodedAuth);
      String body = "{\"type\": \"fs\", \"settings\": {\"location\": \"" + repoLocation + "\"}}";
      HttpEntity entity = new ByteArrayEntity(body.getBytes(StandardCharsets.UTF_8));
      request.setEntity(entity);
      response = httpClient.execute(elastic, request);
      JSONObject jsonResponse = new JSONObject(EntityUtils.toString(response.getEntity()));
      int status = response.getStatusLine().getStatusCode();
      if (status == 200) {
        LOGGER.info("Setup snapshot repo:{} with location:{}", repoName, repoLocation);
      } else {
        throw new IllegalStateException("Could not setup snapshot repo:" + jsonResponse.getJSONObject("error"));
      }
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }
  
  public static void deleteSnapshotRepo(CloseableHttpClient httpClient, HttpHost elastic, String elasticUser,
                                        String elasticPass, String repoName)
    throws IOException, URISyntaxException {
    CloseableHttpResponse response = null;
    try {
      URIBuilder uriBuilder = new URIBuilder()
        .setPathSegments("_snapshot", repoName);
      HttpDelete request = new HttpDelete(uriBuilder.build());
      request.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
      String encodedAuth = Base64.getEncoder().encodeToString((elasticUser + ":" + elasticPass).getBytes());
      request.addHeader(HttpHeaders.AUTHORIZATION, "Basic " + encodedAuth);
      response = httpClient.execute(elastic, request);
      JSONObject jsonResponse = new JSONObject(EntityUtils.toString(response.getEntity()));
      int status = response.getStatusLine().getStatusCode();
      if (status == 200) {
        LOGGER.info("Delete snapshot repo:{}", repoName);
      } else {
        throw new IllegalStateException("Could not delete snapshot:" + jsonResponse.getJSONObject("error"));
      }
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }
  
  public static void takeSnapshot(CloseableHttpClient httpClient, HttpHost elastic, String elasticUser,
                                  String elasticPass, String repoName, String snapshotName, boolean ignoreUnavailable,
                                  String... indices)
    throws URISyntaxException, IOException {
    CloseableHttpResponse response = null;
    try {
      URIBuilder uriBuilder = new URIBuilder();
      uriBuilder
        .setPathSegments("_snapshot", repoName, snapshotName)
        .setParameter("wait_for_completion", "true");
      HttpPut request = new HttpPut(uriBuilder.build());
      request.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
      String encodedAuth = Base64.getEncoder().encodeToString((elasticUser + ":" + elasticPass).getBytes());
      request.addHeader(HttpHeaders.AUTHORIZATION, "Basic " + encodedAuth);
      String body = "{\"indices\":\"" + String.join(",", indices) + "\", " +
        "\"include_global_state\":false, \"ignore_unavailable\":" + ignoreUnavailable + "}";
      HttpEntity entity = new ByteArrayEntity(body.getBytes(StandardCharsets.UTF_8));
      request.setEntity(entity);
      response = httpClient.execute(elastic, request);
      JSONObject jsonResponse = new JSONObject(EntityUtils.toString(response.getEntity()));
      int status = response.getStatusLine().getStatusCode();
      if (status == 200) {
        LOGGER.info("Take snapshot:{} in repo:{}", snapshotName, repoName);
      } else {
        throw new IllegalStateException("Could not take snapshot:" + jsonResponse.getJSONObject("error"));
      }
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }
  
  public static void restoreSnapshot(CloseableHttpClient httpClient, HttpHost elastic, String elasticUser,
                                     String elasticPass, String repoName, String snapshotName, String[] indices,
                                     boolean ignoreUnavailable)
    throws URISyntaxException, IOException {
    CloseableHttpResponse response = null;
    try {
      URIBuilder uriBuilder = new URIBuilder();
      uriBuilder
        .setPathSegments("_snapshot", repoName, snapshotName, "_restore") ;
      HttpPost request = new HttpPost(uriBuilder.build());
      request.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
      String encodedAuth = Base64.getEncoder().encodeToString((elasticUser + ":" + elasticPass).getBytes());
      request.addHeader(HttpHeaders.AUTHORIZATION, "Basic " + encodedAuth);
      String body = "{\"indices\":\"" + String.join(",", indices) + "\", " +
        "\"include_global_state\":false, \"ignore_unavailable\":" + ignoreUnavailable + "}";
      HttpEntity entity = new ByteArrayEntity(body.getBytes(StandardCharsets.UTF_8));
      request.setEntity(entity);
      response = httpClient.execute(elastic, request);
      JSONObject jsonResponse = new JSONObject(EntityUtils.toString(response.getEntity()));
      int status = response.getStatusLine().getStatusCode();
      if (status == 200) {
        LOGGER.info("Restore snapshot:{} from repo:{}", snapshotName, repoName);
      } else {
        throw new IllegalStateException("Could not restore snapshot:" + jsonResponse.getJSONObject("error"));
      }
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }
  
  public static void deleteSnapshot(CloseableHttpClient httpClient, HttpHost elastic, String elasticUser,
                                    String elasticPass, String repoName, String snapshotName)
    throws URISyntaxException, IOException {
    CloseableHttpResponse response = null;
    try {
      URIBuilder uriBuilder = new URIBuilder();
      uriBuilder
        .setPathSegments("_snapshot", repoName, snapshotName) ;
      HttpDelete request = new HttpDelete(uriBuilder.build());
      request.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
      String encodedAuth = Base64.getEncoder().encodeToString((elasticUser + ":" + elasticPass).getBytes());
      request.addHeader(HttpHeaders.AUTHORIZATION, "Basic " + encodedAuth);
      response = httpClient.execute(elastic, request);
      JSONObject jsonResponse = new JSONObject(EntityUtils.toString(response.getEntity()));
      int status = response.getStatusLine().getStatusCode();
      if (status == 200) {
        LOGGER.info("Delete snapshot:{} from repo:{}", snapshotName, repoName);
      } else {
        throw new IllegalStateException("Could not delete snapshot:" + jsonResponse.getJSONObject("error"));
      }
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }
}
