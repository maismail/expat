/*
 * This file is part of Expat
 * Copyright (C) 2018, Logical Clocks AB. All rights reserved
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

package io.hops.hopsworks.expat.migrations.kubernetes;

import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.hops.hopsworks.expat.db.DbConnectionFactory;
import io.hops.hopsworks.expat.kubernetes.KubernetesClientFactory;
import io.hops.hopsworks.expat.migrations.MigrateStep;
import io.hops.hopsworks.expat.migrations.MigrationException;
import io.hops.hopsworks.expat.migrations.RollbackException;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class CreateNS implements MigrateStep {
  
  private static final Logger LOGGER = LogManager.getLogger(CreateNS.class);

  @Override
  public void migrate() throws MigrationException {
    KubernetesClient client;
    try {
      client = KubernetesClientFactory.getClient();
    } catch (ConfigurationException e) {
      throw new MigrationException("Cannot read the configuration", e);
    }

    Connection dbConn;
    Statement stmt = null;
    ResultSet resultSet = null;
    try {
      dbConn = DbConnectionFactory.getConnection();
      stmt = dbConn.createStatement();
      resultSet = stmt.executeQuery("SELECT projectname FROM project");

      while (resultSet.next()) {
        String projectName = resultSet.getString(1);
        String nsName = projectName.toLowerCase().replaceAll("[^a-z0-9-]", "-");
        try {
          client.namespaces().resource(new NamespaceBuilder()
              .withNewMetadata()
              .withName(nsName)
              .endMetadata()
              .build()).create();
          LOGGER.info("Namespace " + nsName + " created for project: " + projectName);
        } catch (KubernetesClientException e) {
          LOGGER.error("Could not create Namespace " + nsName + " for project: " + projectName, e);
        }

      }
    } catch (SQLException | ConfigurationException e) {
      throw new MigrationException("Cannot fetch the list of projects from the database", e);
    } finally {
      if (stmt != null) {
        try {
          stmt.close();
        } catch (SQLException e) {
          // Nothing to do here.
        }
      }

      if (resultSet != null) {
        try {
          resultSet.close();
        } catch (SQLException e) {
          // Nothing to do here.
        }
      }
    }
  }

  @Override
  public void rollback() throws RollbackException {
    KubernetesClient client;
    try {
      client = KubernetesClientFactory.getClient();
    } catch (ConfigurationException e) {
      throw new RollbackException("Cannot read the configuration", e);
    }

    Connection dbConn;
    Statement stmt = null;
    ResultSet resultSet = null;
    try {
      dbConn = DbConnectionFactory.getConnection();
      stmt = dbConn.createStatement();
      resultSet = stmt.executeQuery("SELECT projectname FROM project");

      while (resultSet.next()) {
        String projectName = resultSet.getString(1);
        String nsName = projectName.replace("_", "-");
        try {
          Namespace ns = new NamespaceBuilder()
              .withNewMetadata()
              .withName(nsName)
              .endMetadata()
              .build();

          client.namespaces().delete(ns);
          LOGGER.info("Namespace " + nsName + " deleted for project: " + projectName);
        } catch (KubernetesClientException e) {
          LOGGER.error("Could not delete Namespace " + nsName + " for project: " + projectName, e);
        }

      }
    } catch (SQLException | ConfigurationException e) {
      throw new RollbackException("Cannot fetch the list of projects from the database", e);
    } finally {
      if (stmt != null) {
        try {
          stmt.close();
        } catch (SQLException e) {
          // Nothing to do here.
        }
      }

      if (resultSet != null) {
        try {
          resultSet.close();
        } catch (SQLException e) {
          // Nothing to do here.
        }
      }
    }
  }
}
