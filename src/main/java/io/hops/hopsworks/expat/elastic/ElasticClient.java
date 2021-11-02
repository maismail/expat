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
import java.util.Base64;

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
                                    String elasticUser, String elasticPass, String index) throws IOException {
    CloseableHttpResponse response = null;
    try {
      HttpDelete request = new HttpDelete("_template/" + index);
      String encodedAuth = Base64.getEncoder().encodeToString((elasticUser + ":" + elasticPass).getBytes());
      request.addHeader(HttpHeaders.AUTHORIZATION, "Basic " + encodedAuth);
      LOGGER.info("Deleting index template:{}", index);
      response = httpClient.execute(elastic, request);
      JSONObject jsonResponse = new JSONObject(EntityUtils.toString(response.getEntity()));
      int status = response.getStatusLine().getStatusCode();
      if (status == 200) {
        LOGGER.info("Deleted index template:{}", index);
      } else if (status == 404) {
        LOGGER.info("Index template:{} already deleted", index);
      } else {
        LOGGER.info(jsonResponse);
        if (!jsonResponse.getJSONObject("error").get("reason").toString().startsWith("no such index template")) {
          throw new IllegalStateException("Could not delete index template:" + index);
        }
        LOGGER.info("Skipping index template:{} - already deleted", index);
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
  
  public static void reindex(CloseableHttpClient httpClient, HttpHost elastic,
    String elasticUser, String elasticPass, String fromIndex, String toIndex) throws IOException {
    CloseableHttpResponse response = null;
    try {
      HttpPost request = new HttpPost("_reindex");
      request.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
      String encodedAuth = Base64.getEncoder().encodeToString((elasticUser + ":" + elasticPass).getBytes());
      request.addHeader(HttpHeaders.AUTHORIZATION, "Basic " + encodedAuth);
      HttpEntity entity = new ByteArrayEntity(getReindexBody(fromIndex, toIndex).getBytes("UTF-8"));
      request.setEntity(entity);
      LOGGER.info("Reindexing from:{} to:{}", fromIndex, toIndex);
      response = httpClient.execute(elastic, request);
      JSONObject jsonResponse = new JSONObject(EntityUtils.toString(response.getEntity()));
      int status = response.getStatusLine().getStatusCode();
      if (status == 200) {
        LOGGER.info("Reindexed from:{} to:{}", fromIndex, toIndex);
      } else {
        if (!jsonResponse.getJSONObject("error").get("reason").toString().startsWith("no such index")) {
          throw new IllegalStateException("Could not reindex - indices do not exist");
        } else {
          throw new IllegalStateException("Could not reindex - unknown elastic error:"
            + jsonResponse.getJSONObject("error").get("reason"));
        }
      }
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }
  
  private static String getReindexBody(String fromIndex, String toIndex) {
    JsonObject body = new JsonObject();
    JsonObject source = new JsonObject();
    JsonObject dest = new JsonObject();
    body.add("source", source);
    body.add("dest", dest);
    source.addProperty("index", fromIndex);
    dest.addProperty("index", toIndex);
    return new GsonBuilder().create().toJson(body);
  }
  
  public static void createTemplate(CloseableHttpClient httpClient, HttpHost elastic,
                                    String elasticUser, String elasticPass,
                                    String index, String mapping, String indexPattern)
    throws IOException {
    CloseableHttpResponse response = null;
    try {
      HttpPut request = new HttpPut("_template/" + index);
      request.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
      String encodedAuth = Base64.getEncoder().encodeToString((elasticUser + ":" + elasticPass).getBytes());
      request.addHeader(HttpHeaders.AUTHORIZATION, "Basic " + encodedAuth);
      String body = "{\"index_patterns\": [\"" + indexPattern + "\"]," + mapping + "}";
      HttpEntity entity = new ByteArrayEntity(body.getBytes("UTF-8"));
      request.setEntity(entity);
      LOGGER.info("Creating index template:{}", index);
      response = httpClient.execute(elastic, request);
      JSONObject jsonResponse = new JSONObject(EntityUtils.toString(response.getEntity()));
      int status = response.getStatusLine().getStatusCode();
      if (status == 200) {
        LOGGER.info("Created index template:{}", index);
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
    String elasticUser, String elasticPass, String index, String mapping) throws IOException {
    CloseableHttpResponse response = null;
    try {
      HttpPut request = new HttpPut(index);
      request.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
      String encodedAuth = Base64.getEncoder().encodeToString((elasticUser + ":" + elasticPass).getBytes());
      request.addHeader(HttpHeaders.AUTHORIZATION, "Basic " + encodedAuth);
      HttpEntity entity = new ByteArrayEntity(mapping.getBytes("UTF-8"));
      request.setEntity(entity);
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
      } else {
        throw new IllegalStateException("Could not check index existence - unknown elastic error");
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
  
}
