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

public class ExpatModelVersionFacade extends ExpatAbstractFacade<ExpatModelVersion> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExpatModelVersionFacade.class);
  private static final String FIND_BY_MODEL_ID_AND_VERSION = "SELECT * FROM hopsworks.model_version " +
    "WHERE model_id = ? AND version = ?";

  private static final String INSERT_MODEL_VERSION = String.format("REPLACE INTO %s " +
      "(model_id,version,user_id,created,description,metrics,program,framework,environment,experiment_id," +
      "experiment_project_name) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    "hopsworks.model_version");
  private Connection connection;
  protected ExpatModelVersionFacade(Class<ExpatModelVersion> entityClass) {
    super(entityClass);
  }

  public ExpatModelVersionFacade(Class<ExpatModelVersion> entityClass, Connection connection) {
    super(entityClass);
    this.connection = connection;
  }

  @Override
  public Connection getConnection() {
    return this.connection;
  }

  @Override
  public String findAllQuery() {
    return "SELECT * FROM hopsworks.model_version";
  }

  @Override
  public String findByIdQuery() {
    return "SELECT * FROM hopsworks.model_version WHERE model_id = ? AND version = ?";
  }

  public ExpatModelVersion findByModelIdAndVersion(Integer modelId, Integer version)
    throws IllegalAccessException, SQLException, InstantiationException {
    List<ExpatModelVersion> modelVersionList = this.findByQuery(FIND_BY_MODEL_ID_AND_VERSION,
      new Object[]{modelId, version}, new JDBCType[]{JDBCType.INTEGER, JDBCType.INTEGER});
    if (modelVersionList.isEmpty()) {
      return null;
    }
    if (modelVersionList.size() > 1) {
      throw new IllegalStateException("More than one results found");
    }
    return modelVersionList.get(0);
  }

  public ExpatModelVersion insertModelVersion(Connection connection, Integer modelId, Integer version, Integer userId,
                                              Long created, String description, String metrics,
                                              String program, String framework, String environment, String experimentId,
                                              String experimentProjectName, boolean dryRun)
    throws SQLException, IllegalAccessException, InstantiationException {
    try (PreparedStatement stmt = connection.prepareStatement(INSERT_MODEL_VERSION)) {
      stmt.setInt(1, modelId);
      stmt.setInt(2, version);
      stmt.setInt(3, userId);
      stmt.setTimestamp(4, new java.sql.Timestamp(created));
      stmt.setString(5, description);
      stmt.setString(6, metrics);
      stmt.setString(7, program);
      stmt.setString(8, framework);
      stmt.setString(9, environment);
      stmt.setString(10, experimentId);
      stmt.setString(11, experimentProjectName);
      if (dryRun) {
        LOGGER.info("Executing: " + stmt);
        return null;
      } else {
        stmt.execute();
        return findByModelIdAndVersion(modelId, version);
      }
    }
  }
}
