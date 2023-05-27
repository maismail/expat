/*
 * This file is part of Expat
 * Copyright (C) 2020, Logical Clocks AB. All rights reserved
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
package io.hops.hopsworks.expat.migrations.dataset;

import io.hops.hopsworks.expat.migrations.MigrateStep;
import io.hops.hopsworks.expat.migrations.MigrationException;
import io.hops.hopsworks.expat.migrations.RollbackException;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

public class FixDatasetPermission implements MigrateStep {
  private static final Logger LOGGER = LoggerFactory.getLogger(FixDatasetPermission.class);
  
  private FixDatasetPermissionHelper fixDatasetPermissionHelper;
  
  private void setup() throws SQLException, ConfigurationException {
    fixDatasetPermissionHelper = new FixDatasetPermissionHelper();
    fixDatasetPermissionHelper.setup();
  }
  
  @Override
  public void migrate() throws MigrationException {
    LOGGER.info("Acl Migration started...");
    try {
      setup();
      fixDatasetPermissionHelper.fixAllProjects();
    } catch (Exception e) {
      throw new MigrationException("Acl error", e);
    } finally {
      fixDatasetPermissionHelper.close();
    }
  }
  
  @Override
  public void rollback() throws RollbackException {
    LOGGER.info("Acl Rollback started...");
    try {
      setup();
      fixDatasetPermissionHelper.rollbackAllProject();
    } catch (Exception e) {
      throw new RollbackException("Acl error", e);
    } finally {
      fixDatasetPermissionHelper.close();
    }
  }
}