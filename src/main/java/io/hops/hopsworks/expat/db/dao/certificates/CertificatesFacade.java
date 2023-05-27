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

package io.hops.hopsworks.expat.db.dao.certificates;

import io.hops.hopsworks.expat.db.dao.user.ExpatUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class CertificatesFacade {

  private static final Logger LOGGER = LoggerFactory.getLogger(CertificatesFacade.class);

  private static final String UPDATE_PWD =
      "UPDATE user_certs SET user_key_pwd=? WHERE projectname=? AND username=?";
  private static final String GET_USER_CERT =
      "SELECT * from user_certs WHERE username = ?";
  private static final String PKI_TABLE_NAME = "pki_certificate";
  private static final String INSERT_PKI_CERTIFICATE = String.format("INSERT INTO %s VALUES(?, ?, ?, ?, ?, ?," +
    " ?)", PKI_TABLE_NAME);
  private static final String GET_PKI_CERTIFICATE = String.format("SELECT * FROM %s WHERE subject = ?", PKI_TABLE_NAME);

  public void updateCertPassword(Connection connection,
                                 ExpatCertificate expatCertificate, String newPassword, boolean dryRun)
      throws SQLException {
    try (PreparedStatement stmt = connection.prepareStatement(UPDATE_PWD)) {

      stmt.setString(1, newPassword);
      stmt.setString(2, expatCertificate.getProjectName());
      stmt.setString(3, expatCertificate.getUsername());

      if (dryRun) {
        LOGGER.info(stmt.toString());
        return;
      }
      stmt.execute();
    }
  }

  public List<ExpatCertificate> getUserCertificates(Connection connection, ExpatUser expatUser) throws SQLException {
    ResultSet rs = null;
    List<ExpatCertificate> certificates = new ArrayList<>();
    try (PreparedStatement stmt = connection.prepareStatement(GET_USER_CERT)) {
      stmt.setString(1, expatUser.getUsername());
      rs = stmt.executeQuery();
      while (rs.next()) {
        certificates.add(new ExpatCertificate(
            rs.getString("projectname"),
            rs.getString("username"),
            rs.getString("user_key_pwd")));
      }
    } finally {
      if (rs != null) {
        rs.close();
      }
    }

    return certificates;
  }

  public void insertPKICertificate(Connection connection, Integer ca, Long serialNumber, Integer status,
      String subject, byte[] certificate, Instant notBefore, Instant notAfter, boolean dryRun) throws SQLException {
    try (PreparedStatement stmt = connection.prepareStatement(INSERT_PKI_CERTIFICATE)) {
      stmt.setInt(1, ca);
      stmt.setLong(2, serialNumber);
      stmt.setInt(3, status);
      stmt.setString(4, subject);
      stmt.setBytes(5, certificate);
      stmt.setTimestamp(6, Timestamp.from(notBefore));
      stmt.setTimestamp(7, Timestamp.from(notAfter));

      if (dryRun) {
        LOGGER.info("Executing: " + stmt);
      } else {
        stmt.execute();
      }
    }
  }

  public boolean exists(Connection connection, String subject, boolean dryRun) throws SQLException {
    try (PreparedStatement stmt = connection.prepareStatement(GET_PKI_CERTIFICATE)) {
      stmt.setString(1, subject);
      if (dryRun) {
        LOGGER.info("DryRun - Executing " + stmt);
      } else {
        if (stmt.execute()) {
          return stmt.getResultSet().next();
        }
      }
      return false;
    }
  }

  public void truncatePKICertificates(Connection connection) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.executeQuery("TRUNCATE TABLE pki_certificate");
    }
  }
}
