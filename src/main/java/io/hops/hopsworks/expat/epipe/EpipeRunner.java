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
package io.hops.hopsworks.expat.epipe;

import io.hops.hopsworks.expat.elastic.ElasticClient;
import org.apache.http.HttpHost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EpipeRunner {
  private static final Logger LOGGER = LogManager.getLogger(EpipeRunner.class);
  
  private static final String REINDEX_PREFIX = "reindex_of = ";
  private static final String REINDEX_ALL = REINDEX_PREFIX + "all";
  private static final String PROJECTS_INDEX = "projects";
  private static final String FEATURESTORE_INDEX = "featurestore";
  private static final String REINDEX_PROJECTS = REINDEX_PREFIX + "project";
  private static final String REINDEX_FEATURESTORE = REINDEX_PREFIX + FEATURESTORE_INDEX;
  
  private final static String GET_HDFS_FILE_PROV_LOG = "SELECT count(*) FROM hops.hdfs_file_provenance_log";
  
  
  private static void updateReindexConfig(String reindexConfigPath,
                                          boolean reindexProjects,
                                          boolean reindexFeaturestore)
    throws IOException {
    Path configFile = Paths.get(reindexConfigPath);
    List<String> configContent = new ArrayList<>(Files.readAllLines(configFile, StandardCharsets.UTF_8));
  
    for (int i = 0; i < configContent.size(); i++) {
      if (configFile.startsWith(REINDEX_PREFIX)) {
        if(reindexProjects && reindexFeaturestore) {
          configContent.set(i, REINDEX_ALL);
        } else if(reindexProjects) {
          configContent.set(i, REINDEX_PROJECTS);
        } else {
          configContent.set(i, REINDEX_FEATURESTORE);
        }
      }
    }
  
    Files.write(configFile, configContent, StandardCharsets.UTF_8);
  }
  
  public static void stopEpipe() throws IOException, InterruptedException {
    LOGGER.info("stopping epipe");
    Process stopEpipe = Runtime.getRuntime().exec("systemctl stop epipe");
    BufferedReader stopEpipeReader = new BufferedReader(new InputStreamReader(stopEpipe.getInputStream()));
    String line = null;
    while ( (line = stopEpipeReader.readLine()) != null) {
      LOGGER.info(line);
    }
    stopEpipe.waitFor();
    Thread.sleep(1000);
  }
  
  public static void reindex(CloseableHttpClient httpClient, HttpHost elastic, String elasticUser, String elasticPass,
                             String epipePath, boolean reindexProjects, boolean reindexFeaturestore)
    throws IOException, InterruptedException {
    LOGGER.info("delete indices");
    if(reindexProjects) {
      ElasticClient.deleteIndex(httpClient, elastic, elasticUser, elasticPass, PROJECTS_INDEX);
    }
    if(reindexFeaturestore) {
      ElasticClient.deleteIndex(httpClient, elastic, elasticUser, elasticPass, FEATURESTORE_INDEX);
    }
  
    LOGGER.info("create indices");
    if(reindexProjects) {
      ElasticClient.createIndex(httpClient, elastic, elasticUser, elasticPass, PROJECTS_INDEX);
    }
    if(reindexFeaturestore) {
      ElasticClient.createIndex(httpClient, elastic, elasticUser, elasticPass, FEATURESTORE_INDEX);
    }
  
    LOGGER.info("reindex config");
    String reindexConfigPath = epipePath + "/conf/config-reindex.ini";
    updateReindexConfig(reindexConfigPath, true, true);
  
    LOGGER.info("reindex");
    ProcessBuilder epipeReindexB = new ProcessBuilder()
      .inheritIO()
      .redirectErrorStream(true)
      .redirectOutput(new File(epipePath + "/epipe-reindex.log"))
      .command(epipePath + "/bin/epipe", "-c", reindexConfigPath);
  
    Map<String, String> env = epipeReindexB.environment();
    env.put("LD_LIBRARY_PATH", "/srv/hops/mysql/lib:${LD_LIBRARY_PATH}");
  
    Process epipeReindex = epipeReindexB.start();
    epipeReindex.waitFor();
    LOGGER.info("reindex completed");
  }
  
  public static void restartEpipe() throws IOException, InterruptedException {
    LOGGER.info("restart epipe");
    Process restartEpipe = Runtime.getRuntime().exec("systemctl restart epipe");
    BufferedReader restartEpipeReader = new BufferedReader(new InputStreamReader(restartEpipe.getInputStream()));
    String line = null;
    while ( (line = restartEpipeReader.readLine()) != null) {
      LOGGER.info(line);
    }
    restartEpipe.waitFor();
  }
  
  public static void run(CloseableHttpClient httpClient, HttpHost elastic, String elasticUser, String elasticPass,
                         String epipePath, boolean reindexProjects, boolean reindexFeaturestore)
    throws IOException, InterruptedException {
    stopEpipe();
    reindex(httpClient, elastic, elasticUser, elasticPass, epipePath, reindexProjects, reindexFeaturestore);
    restartEpipe();
  }
  
  public static void waitForEpipeIdle(Connection connection) throws InterruptedException, SQLException {
    int retries = 15;
    int waitime = 5000;
    while(retries > 0) {
      if(getFileProvLogsSize(connection) == 0) {
        return;
      }
      LOGGER.info("waiting for epipe provenance log to be consumed");
      Thread.sleep(waitime);
      retries--;
      waitime = waitime + 5000;
    }
    LOGGER.info("epipe too slow emptying provenance log");
    throw new IllegalStateException("epipe too slow emptying provenance log");
  }
  
  private static int getFileProvLogsSize(Connection connection) throws SQLException {
    PreparedStatement fileProvLogsStmt = null;
    try {
      fileProvLogsStmt = connection.prepareStatement(GET_HDFS_FILE_PROV_LOG);
      ResultSet allFSResultSet = fileProvLogsStmt.executeQuery();
      if(allFSResultSet.next()) {
        return allFSResultSet.getInt(1);
      } else {
        return -1;
      }
    } finally {
      if(fileProvLogsStmt != null) {
        fileProvLogsStmt.close();
      }
    }
  }
}
