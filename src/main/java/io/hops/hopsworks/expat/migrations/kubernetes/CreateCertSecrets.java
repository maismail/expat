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

import com.google.common.io.Files;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.utils.Serialization;
import io.hops.hopsworks.common.util.HopsUtils;
import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import io.hops.hopsworks.expat.db.DbConnectionFactory;
import io.hops.hopsworks.expat.kubernetes.KubernetesClientFactory;
import io.hops.hopsworks.expat.migrations.MigrateStep;
import io.hops.hopsworks.expat.migrations.MigrationException;
import io.hops.hopsworks.expat.migrations.RollbackException;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static io.hops.hopsworks.common.util.Settings.CERT_PASS_SUFFIX;
import static io.hops.hopsworks.common.util.Settings.KEYSTORE_SUFFIX;
import static io.hops.hopsworks.common.util.Settings.TRUSTSTORE_SUFFIX;

public class CreateCertSecrets implements MigrateStep {

  private static final Logger LOGGER = LoggerFactory.getLogger(CreateCertSecrets.class);

  @Override
  public void migrate() throws MigrationException {
    /*KubernetesClient client;
    try {
      client = KubernetesClientFactory.getClient();
    } catch (ConfigurationException e) {
      throw new MigrationException("Cannot read the configuration", e);
    }*/
    
    File baseDir = new File("/tmp/k8s");
    baseDir.mkdirs();
    
    String masterPwd = null;
    try {
      Configuration config = ConfigurationBuilder.getConfiguration();
      Path masterPwdPath = Paths.get(config.getString(ExpatConf.MASTER_PWD_FILE_KEY));
      masterPwd = Files.toString(masterPwdPath.toFile(), Charset.defaultCharset());
    } catch (ConfigurationException | IOException e) {
      throw new MigrationException("Could not read the master password", e);
    }

    Connection dbConn;
    Statement stmt = null;
    ResultSet resultSet = null;
    try {
      dbConn = DbConnectionFactory.getConnection();
      stmt = dbConn.createStatement();
      resultSet = stmt.executeQuery("SELECT u.username AS username, projectname, password, user_key, " +
          "user_cert, user_key_pwd FROM users u join user_certs uc ON u.username = uc.username");

      while (resultSet.next()) {
        String projectName = resultSet.getString("projectname");
        String nsName = projectName.toLowerCase().replaceAll("[^a-z0-9-]", "-");

        String kubeUsername = nsName + "--" +
            resultSet.getString("username").toLowerCase().replaceAll("[^a-z0-9]", "-");
        // In the cluster we have usernames that, after the replace, end with -. In this case we add a 0 after it
        if (kubeUsername.endsWith("-")) {
          kubeUsername = kubeUsername + "0";
        }
        String hopsUsername = projectName + "__" + resultSet.getString("username");

        try {
          String certPwd = HopsUtils.decrypt(resultSet.getString("password"),
              resultSet.getString("user_key_pwd"), masterPwd);


          Map<String, String> secretData = new HashMap<>();
          secretData.put(hopsUsername + CERT_PASS_SUFFIX, Base64.getEncoder().encodeToString(certPwd.getBytes()));
          secretData.put(hopsUsername + KEYSTORE_SUFFIX,
              Base64.getEncoder().encodeToString(resultSet.getBytes("user_key")));
          secretData.put(hopsUsername + TRUSTSTORE_SUFFIX,
              Base64.getEncoder().encodeToString(resultSet.getBytes("user_cert")));

          Secret secret = new SecretBuilder()
              .withMetadata(new ObjectMetaBuilder()
                  .withName(kubeUsername)
                  .build())
              .withData(secretData)
              .build();
          
          String yamlString = Serialization.asYaml(secret);
          
          File secretFile = new File(baseDir, kubeUsername + ".yaml");
          
          // Write the YAML to a file
          try (FileWriter writer = new FileWriter(secretFile)) {
            writer.write(yamlString);
          } catch (IOException e) {
            e.printStackTrace();
          }
          
          // Send request
          //client.secrets().inNamespace(nsName).createOrReplace(secret);

          LOGGER.info("Secret " + kubeUsername + " created for project user: " + projectName);
        } catch (Exception e) {
          LOGGER.error("Could not create secret " + kubeUsername + " for project user: "
              + projectName, e);
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
      resultSet = stmt.executeQuery("SELECT projectname, username FROM project");

      while (resultSet.next()) {
        String projectName = resultSet.getString("projectname");
        String nsName = projectName.replace("_", "-");

        String kubeUsername = nsName + "--" + resultSet.getString("username");
        String hopsUsername = projectName + "__" + resultSet.getString("username");

        try {

          Secret secret = new SecretBuilder()
              .withMetadata(new ObjectMetaBuilder()
                  .withName(kubeUsername)
                  .build())
              .build();

          client.secrets().inNamespace(nsName).delete(secret);
          LOGGER.info("Secret " + kubeUsername + " deleted for project user: " + hopsUsername);
        } catch (KubernetesClientException e) {
          LOGGER.error("Could not delete secret" + kubeUsername + " for project user: " +
              hopsUsername, e);
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
