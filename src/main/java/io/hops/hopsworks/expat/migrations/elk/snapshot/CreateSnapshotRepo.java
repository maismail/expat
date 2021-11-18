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

import io.hops.hopsworks.expat.configuration.ExpatConf;
import io.hops.hopsworks.expat.elastic.ElasticClient;
import io.hops.hopsworks.expat.migrations.MigrationException;
import io.hops.hopsworks.expat.migrations.RollbackException;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;

public class CreateSnapshotRepo extends SnapshotRepo {
  
  private void setupSnapshotRepo() throws NoSuchAlgorithmException, KeyStoreException, ConfigurationException,
                                          KeyManagementException, IOException, URISyntaxException {
    LOGGER.info("create elastic snapshot repo");
    setup();
    if (snapshotRepoName == null) {
      throw new ConfigurationException(ExpatConf.ELASTIC_SNAPSHOT_REPO_NAME + " cannot be null");
    }
    if (snapshotRepoLocation == null) {
      throw new ConfigurationException(ExpatConf.ELASTIC_SNAPSHOT_REPO_LOCATION + " cannot be null");
    }
    ElasticClient.createSnapshotRepo(httpClient, elastic, elasticUser, elasticPass,
      snapshotRepoName, snapshotRepoLocation);
  }
  
  @Override
  public void migrate() throws MigrationException {
    Throwable tAux = null;
    try {
      setupSnapshotRepo();
    } catch (Throwable t) {
      tAux = t;
      throw new MigrationException("error", t);
    } finally {
      try {
        close();
      } catch (IOException e) {
        if(tAux != null) {
          throw new MigrationException("error", e);
        }
      }
    }
  }
  
  @Override
  public void rollback() throws RollbackException {
    Throwable tAux = null;
    try {
      setupSnapshotRepo();
    } catch (Throwable t) {
      tAux = t;
      throw new RollbackException("error", t);
    } finally {
      try {
        close();
      } catch (IOException e) {
        if(tAux != null) {
          throw new RollbackException("error", e);
        }
      }
    }
  }
}
