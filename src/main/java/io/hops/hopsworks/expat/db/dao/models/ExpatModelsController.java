/*
 * This file is part of Expat
 * Copyright (C) 2023, Hopsworks AB. All rights reserved
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;


public class ExpatModelsController {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExpatModelsController.class);

  private ExpatModelFacade modelFacade;
  private ExpatModelVersionFacade modelVersionFacade;

  public ExpatModelsController(Connection connection) {
    this.modelFacade = new ExpatModelFacade(ExpatModel.class, connection);
    this.modelVersionFacade = new ExpatModelVersionFacade(ExpatModelVersion.class, connection);
  }

  public ExpatModel getByProjectAndName(Integer projectId, String name) throws SQLException,
    IllegalAccessException, InstantiationException {
    return modelFacade.findByProjectAndName(projectId, name);
  }

  public ExpatModel insertModel(Connection connection, String name, Integer projectId, boolean dryRun)
    throws SQLException, IllegalAccessException, InstantiationException {
    return modelFacade.insertModel(connection, name, projectId, dryRun);
  }

  public ExpatModelVersion insertModelVersion(Connection connection, Integer modelId, Integer version, Integer userId,
                                              Long created, String description, String metrics,
                                              String program, String framework, String environment, String experimentId,
                                              String experimentProjectName, boolean dryRun)
    throws SQLException, IllegalAccessException, InstantiationException {
    return modelVersionFacade.insertModelVersion(connection, modelId, version, userId, created, description,
      metrics, program, framework, environment, experimentId, experimentProjectName, dryRun);
  }
}
