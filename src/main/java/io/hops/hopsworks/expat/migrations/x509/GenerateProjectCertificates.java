/**
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
package io.hops.hopsworks.expat.migrations.x509;

import io.hops.hopsworks.common.util.HopsUtils;
import io.hops.hopsworks.expat.db.dao.certificates.ExpatCertificate;
import io.hops.hopsworks.expat.db.dao.user.ExpatUser;
import io.hops.hopsworks.expat.migrations.MigrateStep;
import io.hops.hopsworks.expat.migrations.MigrationException;
import io.hops.hopsworks.expat.migrations.RollbackException;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class GenerateProjectCertificates extends GenerateCertificates implements MigrateStep {
  private static final Logger LOGGER = LoggerFactory.getLogger(GenerateProjectCertificates.class);
  private static final String SELECT_PROJECT_CERTS = "SELECT * FROM projectgenericuser_certs";
  private static final String SELECT_PROJECT_BY_NAME = "SELECT * FROM project WHERE projectname = ?";
  private final static String UPDATE_PROJECT_CERTS = "UPDATE projectgenericuser_certs SET pgu_key = ?, " +
      "pgu_cert = ?, cert_password = ? WHERE project_generic_username = ?";
  
  
  @Override
  public void migrate() throws MigrationException {
    try {
      // Important!
      setup("ProjectCertificates");
      
      LOGGER.info("Getting all Project Certificates");
      Map<ExpatCertificate, ExpatUser> projectCerts = getProjectCerts();
      
      generateNewCertsAndUpdateDb(projectCerts, "Project Generic");
  
      LOGGER.info("Finished migration of User Certificates.");
      LOGGER.info(">>> You should revoke certificates and clean manually backup dir with previous certs: " +
          certsBackupDir.toString());
    } catch (ConfigurationException | SQLException ex) {
      String errorMsg = "Could not initialize database connection";
      LOGGER.error(errorMsg, ex);
      throw new MigrationException(errorMsg, ex);
    } catch (IOException ex) {
      LOGGER.error(ex.getMessage(), ex);
      throw new MigrationException(ex.getMessage(), ex);
    } catch (Exception ex) {
      String errorMsg = "Generic error";
      LOGGER.error(errorMsg, ex);
      throw new MigrationException(errorMsg, ex);
    }
  }
  
  private Map<ExpatCertificate, ExpatUser> getProjectCerts() throws Exception {
    Map<ExpatCertificate, ExpatUser> projectCerts = new HashMap<>();
    ResultSet certsRS = null, projectRS = null;
    PreparedStatement projectStmt = null;
    Statement certsStmt = connection.createStatement();
    try {
      certsRS = certsStmt.executeQuery(SELECT_PROJECT_CERTS);
      while (certsRS.next()) {
        String projectGenericUN = certsRS.getString("project_generic_username");
        String[] tokens = projectGenericUN.split("__");
        if (tokens.length != 2) {
          throw new MigrationException("Could not parse Project Generic Username: " + projectGenericUN);
        }
        String projectName = tokens[0];
        ExpatCertificate cert = new ExpatCertificate(projectName, "PROJECTGENERICUSER");
  
        LOGGER.info("Processing: " + projectGenericUN + " <" + tokens[0] + ", " + tokens[1] + ">");
        // Get owner of the project
        try {
          projectStmt = connection.prepareStatement(SELECT_PROJECT_BY_NAME);
          projectStmt.setString(1, projectName);
          projectRS = projectStmt.executeQuery();
          if (!projectRS.next()) {
            LOGGER.warn("Could not find project " + projectName);
            continue;
          }
          String ownerEmail = projectRS.getString("username");
          ExpatUser user = expatUserFacade.getExpatUserByEmail(connection, ownerEmail);
          cert.setPlainPassword(HopsUtils.randomString(64));
          String cipherPassword = HopsUtils.encrypt(user.getPassword(), cert.getPlainPassword(), masterPassword);
          cert.setCipherPassword(cipherPassword);
          
          projectCerts.put(cert, user);
        } finally {
          if (projectRS != null) {
            projectRS.close();
          }
          if (projectStmt != null) {
            projectStmt.close();
          }
        }
      }
      return projectCerts;
    } finally {
      if (certsRS != null) {
        certsRS.close();
      }
      if (certsStmt != null) {
        certsStmt.close();
      }
    }
  }
  
  @Override
  public void rollback() throws RollbackException {
    try {
      migrate();
    } catch (MigrationException ex) {
      throw new RollbackException("Could not rollback Project Certificates", ex);
    }
  }
  
  void updateCertificatesInDB(Set<ExpatCertificate> certificates, Connection connection)
    throws SQLException {
    PreparedStatement updateStmt = null;
    try {
      connection.setAutoCommit(false);
      updateStmt = connection.prepareStatement(UPDATE_PROJECT_CERTS);
      for (ExpatCertificate c : certificates) {
        updateStmt.setBytes(1, c.getKeyStore());
        updateStmt.setBytes(2, c.getTrustStore());
        updateStmt.setString(3, c.getCipherPassword());
        String pgu = c.getProjectName() + "__" + c.getUsername();
        updateStmt.setString(4, pgu);
        updateStmt.addBatch();
        LOGGER.debug("Added " + c + " to Tx batch");
      }
      updateStmt.executeBatch();
      connection.commit();
      LOGGER.info("Finished updating database");
    } finally {
      if (updateStmt != null) {
        updateStmt.close();
      }
      connection.setAutoCommit(true);
    }
  }
}
