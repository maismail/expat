/**
 * This file is part of Expat
 * Copyright (C) 2022, Logical Clocks AB. All rights reserved
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

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class KeysFacade {
  private static final Logger LOGGER = LogManager.getLogger(KeysFacade.class);
  private static final String TABLE_NAME = "pki_key";
  private static final String INSERT_KEY = String.format("INSERT INTO %s VALUES(?, ?, ?)", TABLE_NAME);
  private static final String GET_KEY = String.format("SELECT * FROM %s WHERE owner = ?", TABLE_NAME);
  private final Connection connection;
  private final boolean dryRun;

  public KeysFacade(Connection connection, boolean dryRun) {
    this.connection = connection;
    this.dryRun = dryRun;
  }

  public void insertKey(String owner, Integer type, byte[] key) throws SQLException {
    try (PreparedStatement stmt = connection.prepareStatement(INSERT_KEY)) {
      stmt.setString(1, owner.toUpperCase());
      stmt.setInt(2, type);
      stmt.setBytes(3, key);

      if (dryRun) {
        LOGGER.log(Level.INFO, "Executing " + stmt);
      } else {
        stmt.execute();
      }
    }
  }

  public boolean exists(String owner) throws SQLException {
    try (PreparedStatement stmt = connection.prepareStatement(GET_KEY)) {
      stmt.setString(1, owner);
      if (dryRun) {
        LOGGER.log(Level.INFO, "DryRun - Executing " + stmt);
      } else {
        if (stmt.execute()) {
          return stmt.getResultSet().next();
        }
      }
      return false;
    }
  }

  public void truncate() throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.executeQuery("TRUNCATE TABLE pki_key");
    }
  }
}
