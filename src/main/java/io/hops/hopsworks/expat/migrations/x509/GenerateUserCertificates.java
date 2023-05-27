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


public class GenerateUserCertificates extends GenerateCertificates implements MigrateStep {
  private static final Logger LOGGER = LoggerFactory.getLogger(GenerateProjectCertificates.class);
  private final static String SELECT_USER_CERTS = "SELECT * FROM user_certs";
  private final static String UPDATE_USER_CERTS =
    "UPDATE user_certs SET user_key = ?, user_cert = ?, user_key_pwd = ? WHERE projectname = ? && username = ?";
  
  @Override
  public void migrate() throws MigrationException {
    try {
      // Important!
      setup("UserCertificates");
      
      LOGGER.info("Getting all User Certificates");
      Map<ExpatCertificate, ExpatUser> userCerts = getUserCerts();
      
      generateNewCertsAndUpdateDb(userCerts, "User");
      
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
  
  @Override
  public void rollback() throws RollbackException {
    try {
      migrate();
    } catch (MigrationException ex) {
      throw new RollbackException("Could not rollback User Certificates", ex);
    }
  }

  private Map<ExpatCertificate, ExpatUser> getUserCerts() throws Exception {
    Map<ExpatCertificate, ExpatUser> userCerts = new HashMap<>();
    ResultSet certsRS = null;
    Statement userCertsStmt = connection.createStatement();
    try {
      certsRS = userCertsStmt.executeQuery(SELECT_USER_CERTS);
      
      while (certsRS.next()) {
        String projectName = certsRS.getString("projectname");
        String username = certsRS.getString("username");
    
        ExpatCertificate cert = new ExpatCertificate(projectName, username);
        ExpatUser user = expatUserFacade.getExpatUserByUsername(connection, username);
        cert.setPlainPassword(HopsUtils.randomString(64));
        String cipherPassword = HopsUtils.encrypt(user.getPassword(), cert.getPlainPassword(), masterPassword);
        cert.setCipherPassword(cipherPassword);
        userCerts.put(cert, user);
      }
      return userCerts;
    } finally {
      if (certsRS != null) {
        certsRS.close();
      }
      if (userCertsStmt != null) {
        userCertsStmt.close();
      }
    }
  }
  
  void updateCertificatesInDB(Set<ExpatCertificate> userCerts, Connection conn) throws SQLException {
    
    PreparedStatement updateStmt = null;
    try {
      conn.setAutoCommit(false);
      updateStmt = conn.prepareStatement(UPDATE_USER_CERTS);
      for (ExpatCertificate uc : userCerts) {
        updateStmt.setBytes(1, uc.getKeyStore());
        updateStmt.setBytes(2, uc.getTrustStore());
        updateStmt.setString(3, uc.getCipherPassword());
        updateStmt.setString(4, uc.getProjectName());
        updateStmt.setString(5, uc.getUsername());
        updateStmt.addBatch();
        LOGGER.debug("Added " + uc + " to Tx batch");
      }
      updateStmt.executeBatch();
      conn.commit();
      LOGGER.info("Finished updating database");
    } finally {
      if (updateStmt != null) {
        updateStmt.close();
      }
      conn.setAutoCommit(true);
    }
  }
}
