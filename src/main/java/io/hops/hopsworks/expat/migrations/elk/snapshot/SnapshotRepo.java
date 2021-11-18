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
package io.hops.hopsworks.expat.migrations.elk.snapshot;

import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import io.hops.hopsworks.expat.migrations.MigrateStep;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.http.HttpHost;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;

public abstract class SnapshotRepo implements MigrateStep {
  protected final static Logger LOGGER = LogManager.getLogger(SnapshotRepo.class);
  
  protected HttpHost elastic;
  protected String elasticUser;
  protected String elasticPass;
  protected CloseableHttpClient httpClient;
  
  protected String snapshotRepoName = null;
  protected String snapshotRepoLocation = null;
  protected String snapshotName = null;
  protected String[] snapshotIndices = null;
  protected Boolean ignoreUnavailable = null;
  
  protected void setup()
    throws ConfigurationException, KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
    
    Configuration conf = ConfigurationBuilder.getConfiguration();
    
    String elasticURI = conf.getString(ExpatConf.ELASTIC_URI);
    if (elasticURI == null) {
      throw new ConfigurationException(ExpatConf.ELASTIC_URI + " cannot be null");
    }
    elastic = HttpHost.create(elasticURI);
  
    elasticUser = conf.getString(ExpatConf.ELASTIC_USER_KEY);
    if (elasticUser == null) {
      throw new ConfigurationException(ExpatConf.ELASTIC_USER_KEY + " cannot be null");
    }
    elasticPass = conf.getString(ExpatConf.ELASTIC_PASS_KEY);
    if (elasticPass == null) {
      throw new ConfigurationException(ExpatConf.ELASTIC_PASS_KEY + " cannot be null");
    }
  
    snapshotRepoLocation = conf.getString(ExpatConf.ELASTIC_SNAPSHOT_REPO_LOCATION);
    snapshotRepoName = conf.getString(ExpatConf.ELASTIC_SNAPSHOT_REPO_NAME);
    snapshotName = conf.getString(ExpatConf.ELASTIC_SNAPSHOT_NAME);
    snapshotIndices = conf.getStringArray(ExpatConf.ELASTIC_SNAPSHOT_INDICES);
    ignoreUnavailable = conf.getBoolean(ExpatConf.ELASTIC_SNAPSHOT_IGNORE_UNAVAILABLE);
  
    httpClient = HttpClients
      .custom()
      .setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(
        CookieSpecs.IGNORE_COOKIES).build())
      .setSSLContext(new SSLContextBuilder().loadTrustMaterial((x509Certificates, s) -> true).build())
      .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
      .build();
  }
  
  protected void close() throws IOException {
    if(httpClient != null) {
      httpClient.close();
    }
  }
}
