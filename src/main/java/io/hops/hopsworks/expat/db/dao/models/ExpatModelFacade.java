/*
 * This file is part of Expat
 * Copyright (C) 2023, Logical Clocks AB. All rights reserved
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
package io.hops.hopsworks.expat.db.dao.models;

import io.hops.hopsworks.expat.db.dao.ExpatAbstractFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class ExpatModelFacade extends ExpatAbstractFacade<ExpatModel> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExpatModelFacade.class);
  private static final String FIND_BY_PROJECT_AND_NAME = "SELECT * FROM hopsworks.model " +
    "WHERE project_id = ? AND name = ?";

  private static final String INSERT_MODEL = String.format("INSERT IGNORE INTO %s (name,project_id) VALUES(?, ?)",
    "hopsworks.model");
  private Connection connection;
  protected ExpatModelFacade(Class<ExpatModel> entityClass) {
    super(entityClass);
  }

  public ExpatModelFacade(Class<ExpatModel> entityClass, Connection connection) {
    super(entityClass);
    this.connection = connection;
  }

  @Override
  public Connection getConnection() {
    return this.connection;
  }

  @Override
  public String findAllQuery() {
    return "SELECT * FROM hopsworks.model";
  }

  @Override
  public String findByIdQuery() {
    return "SELECT * FROM hopsworks.model WHERE id = ?";
  }

  public ExpatModel findByProjectAndName(Integer projectId, String name)
    throws IllegalAccessException, SQLException, InstantiationException {
    List<ExpatModel> modelList = this.findByQuery(FIND_BY_PROJECT_AND_NAME, new Object[]{projectId, name},
      new JDBCType[]{JDBCType.INTEGER, JDBCType.VARCHAR});
    if (modelList.isEmpty()) {
      return null;
    }
    if (modelList.size() > 1) {
      throw new IllegalStateException("More than one results found");
    }
    return modelList.get(0);
  }

  public ExpatModel insertModel(Connection connection, String name, Integer projectId, boolean dryRun)
    throws SQLException, IllegalAccessException, InstantiationException {
    try (PreparedStatement stmt = connection.prepareStatement(INSERT_MODEL)) {
      stmt.setString(1, name);
      stmt.setInt(2, projectId);
      if (dryRun) {
        LOGGER.info("Executing: " + stmt);
        return null;
      } else {
        stmt.execute();
        return findByProjectAndName(projectId, name);
      }
    }
  }
}
