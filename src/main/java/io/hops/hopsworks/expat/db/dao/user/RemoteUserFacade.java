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

package io.hops.hopsworks.expat.db.dao.user;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class RemoteUserFacade {
  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteUserFacade.class);

  private final static String INSERT_REMOTE = "INSERT INTO remote_user(type, auth_key, uuid, uid) values (?, ?, ?, ?);";

  public void insertRemoteUser(Connection connection, RemoteUser remoteUser, boolean dryRun) throws SQLException {
    try (PreparedStatement insertStmt = connection.prepareStatement(INSERT_REMOTE)) {
      insertStmt.setInt(1, remoteUser.getType());
      insertStmt.setString(2, remoteUser.getAuth_key());
      insertStmt.setString(3, remoteUser.getUuid());
      insertStmt.setInt(4, remoteUser.getUid());

      if (dryRun) {
        LOGGER.info(insertStmt.toString());
        return;
      }
      insertStmt.execute();
    }
  }
}
